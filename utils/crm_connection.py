#!/usr/bin/env python3
"""
Подключение к BPM Account (SQL Server) через ODBC DSN=CRMDB.

На vz2/vz3: /etc/odbc.ini [CRMDB] → BPMonline_CRM.
Локальный pyodbc; при недоступности — CRM_SSH_HOST (fallback).
Для старого SQL Server на vz2: OPENSSL_CONF=/etc/ssl/openssl-mssql.cnf
"""

from __future__ import annotations

import csv
import io
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, Optional

_OPENSSL_MSSQL = "/etc/ssl/openssl-mssql.cnf"
if os.path.isfile(_OPENSSL_MSSQL) and not os.environ.get("OPENSSL_CONF"):
    os.environ["OPENSSL_CONF"] = _OPENSSL_MSSQL

# Creatio SubjectTypeId (см. iridium_sync.extract_crm)
SUBJECT_LEGAL = "E0B9A33C-1785-4E03-9580-6146B43981BD"
SUBJECT_PERSON = "1FDE0ABF-AE66-4609-85C3-9170C3480EE8"

ACCOUNT_SQL = """
SELECT
    a.Id, a.Name, a.Code1C, a.Phone, a.Web,
    a.PassportId, a.IssuedBy, a.IssuedDate, a.Registration,
    a.SubjectTypeId, a.IDNumber, a.KPP,
    c.Name AS PrimaryContactName,
    c.JobTitle AS PrimaryContactJobTitle,
    o.Name AS OwnerContactName,
    o.Email AS OwnerContactEmail
FROM Account a
LEFT JOIN Contact c ON c.Id = a.PrimaryContactId
LEFT JOIN Contact o ON o.Id = a.OwnerId
WHERE a.Code1C IS NOT NULL AND LTRIM(RTRIM(a.Code1C)) <> ''
"""

# Ответственный менеджер СТЭККОМ на карточке Account (OwnerId → Contact).
STECOM_MANAGER_EMAIL_DOMAIN = "@steccom.ru"

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def is_valid_email(email: Optional[str]) -> bool:
    if not email:
        return False
    email = str(email).strip()
    if not _EMAIL_RE.match(email):
        return False
    if email.startswith((".", "_", "-")) or email.endswith((".", "_", "-")):
        return False
    if ".." in email or email.startswith("@") or email.endswith("@"):
        return False
    local, _, domain = email.partition("@")
    if not local or "." not in domain:
        return False
    if domain.startswith((".", "-")) or domain.endswith((".", "-")):
        return False
    return True


def _crm_env() -> dict:
    return {
        "dsn": os.getenv("CRM_DSN", "CRMDB"),
        "user": os.getenv("CRM_USER", "bpm"),
        "password": os.getenv("CRM_PASSWORD", "bpm"),
        "ssh_host": os.getenv("CRM_SSH_HOST", "").strip(),
    }


def connect_crm_odbc():
    """Локальный pyodbc DSN (vz3 или хост с /etc/odbc.ini)."""
    import pyodbc

    cfg = _crm_env()
    return pyodbc.connect(f"DSN={cfg['dsn']};UID={cfg['user']};PWD={cfg['password']}")


def _normalize_account_row(rec: dict) -> dict:
    code = str(rec.get("Code1C") or "").strip()
    rec["Code1C"] = code
    st = str(rec.get("SubjectTypeId") or "").upper()
    rec["is_legal"] = st == SUBJECT_LEGAL.upper()
    rec["is_person"] = st == SUBJECT_PERSON.upper()
    web = rec.get("Web")
    rec["email"] = str(web).strip() if is_valid_email(web) else None
    pid = rec.get("PassportId")
    rec["passport_id"] = str(pid).strip() if pid is not None and str(pid).strip() else None
    rec["has_passport"] = bool(rec["passport_id"])
    pcn = rec.get("PrimaryContactName")
    rec["primary_contact_name"] = (
        str(pcn).strip() if pcn is not None and str(pcn).strip() else None
    )
    pjt = rec.get("PrimaryContactJobTitle")
    rec["primary_contact_job"] = (
        str(pjt).strip() if pjt is not None and str(pjt).strip() else None
    )
    owner_name = rec.get("OwnerContactName")
    owner_email = rec.get("OwnerContactEmail")
    owner_name_s = (
        str(owner_name).strip() if owner_name is not None and str(owner_name).strip() else None
    )
    owner_email_s = (
        str(owner_email).strip().lower()
        if owner_email is not None and str(owner_email).strip()
        else None
    )
    if owner_name_s and owner_email_s and owner_email_s.endswith(STECOM_MANAGER_EMAIL_DOMAIN):
        rec["manager_fio"] = owner_name_s
    else:
        rec["manager_fio"] = None
    return rec


CONTACTS_SQL = """
SELECT
    LTRIM(RTRIM(a.Code1C)) AS Code1C,
    c.Name AS ContactName,
    c.JobTitle AS JobTitle,
    c.CreatedOn AS CreatedOn
FROM Contact c
INNER JOIN Account a ON c.AccountId = a.Id
WHERE a.Code1C IS NOT NULL AND LTRIM(RTRIM(a.Code1C)) <> ''
  AND c.Name IS NOT NULL AND LTRIM(RTRIM(c.Name)) <> ''
"""


def _job_director_score(job_title: Optional[str]) -> int:
    """Приоритет должности для LETTER_FIO (ЮЛ). 0 = не берём."""
    job = (job_title or "").lower().replace("ё", "е")
    if not job.strip():
        return 0
    if "генеральн" in job and "директор" in job:
        return 100
    if "директор" in job:
        return 50
    if "руководител" in job:
        return 20
    return 0


def _pick_director_contact(contacts: list) -> tuple[Optional[str], Optional[str]]:
    """
    Из списка контактов аккаунта выбрать ФИО директора.
    Возвращает (name, job_title).
    """
    ranked = []
    for c in contacts:
        name = (c.get("name") or "").strip()
        if len(name) < 5:
            continue
        score = _job_director_score(c.get("job_title"))
        if score <= 0:
            continue
        created = c.get("created_on")
        ranked.append((score, created or 0, name, (c.get("job_title") or "").strip()))
    if not ranked:
        return None, None
    # выше score, затем более новый CreatedOn
    ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return ranked[0][2], ranked[0][3]


def _attach_account_contacts(by_code: Dict[str, dict], cur) -> None:
    """Дополнить Account контактами Contact.AccountId → director_contact_name."""
    from collections import defaultdict
    from datetime import datetime

    cur.execute(CONTACTS_SQL)
    cols = [c[0] for c in cur.description]
    by_acc: dict[str, list] = defaultdict(list)
    for row in cur.fetchall():
        rec = dict(zip(cols, row))
        code = str(rec.get("Code1C") or "").strip()
        if not code:
            continue
        created = rec.get("CreatedOn")
        if isinstance(created, datetime):
            created_key = created.timestamp()
        else:
            created_key = 0
        by_acc[code].append(
            {
                "name": str(rec.get("ContactName") or "").strip(),
                "job_title": str(rec.get("JobTitle") or "").strip(),
                "created_on": created_key,
            }
        )
    for code, acc in by_code.items():
        name, job = _pick_director_contact(by_acc.get(code, []))
        acc["director_contact_name"] = name
        acc["director_contact_job"] = job
        # для отображения: если Primary пуст — покажем выбранного директора
        if name and not acc.get("primary_contact_name"):
            acc["primary_contact_name"] = name
            acc["primary_contact_job"] = job


def fetch_accounts_odbc() -> Dict[str, dict]:
    conn = connect_crm_odbc()
    cur = conn.cursor()
    try:
        cur.execute(ACCOUNT_SQL)
        cols = [c[0] for c in cur.description]
        by_code: Dict[str, dict] = {}
        for row in cur.fetchall():
            rec = _normalize_account_row(dict(zip(cols, row)))
            if rec["Code1C"]:
                by_code[rec["Code1C"]] = rec
        _attach_account_contacts(by_code, cur)
        return by_code
    finally:
        cur.close()
        conn.close()


_PERL_FETCH = r"""
use strict;
use warnings;
use DBI;
binmode(STDOUT, ":encoding(UTF-8)");
my $dsn = $ENV{CRM_DSN} // 'crmdb';
my $user = $ENV{CRM_USER} // 'bpm';
my $pass = $ENV{CRM_PASSWORD} // 'bpm';
my $dbh = DBI->connect("dbi:ODBC:$dsn", $user, $pass, {
  RaiseError => 1, PrintError => 0, LongReadLen => 1_000_000,
});
my $sql = q{
SELECT a.Code1C, a.Name, a.Phone, a.Web, a.PassportId, a.IssuedBy, a.IssuedDate,
       a.Registration, a.SubjectTypeId, a.IDNumber, a.KPP,
       c.Name AS PrimaryContactName, c.JobTitle AS PrimaryContactJobTitle,
       o.Name AS OwnerContactName, o.Email AS OwnerContactEmail
FROM Account a
LEFT JOIN Contact c ON c.Id = a.PrimaryContactId
LEFT JOIN Contact o ON o.Id = a.OwnerId
WHERE a.Code1C IS NOT NULL AND LTRIM(RTRIM(a.Code1C)) <> ''
};
my $sth = $dbh->prepare($sql);
$sth->execute();
my @cols = @{$sth->{NAME}};
print join("\t", map { _esc($_) } @cols), "\n";
while (my $row = $sth->fetchrow_arrayref) {
  print join("\t", map { _esc(defined $_ ? $_ : '') } @$row), "\n";
}
$sth->finish;
$dbh->disconnect;
sub _esc {
  my ($s) = @_;
  $s =~ s/[\t\r\n]+/ /g;
  return $s;
}
"""


def fetch_accounts_ssh(ssh_host: str) -> Dict[str, dict]:
    """Выгрузка Account через SSH на хост с ODBC (обычно vz3)."""
    cfg = _crm_env()
    env_prefix = (
        f"CRM_DSN={cfg['dsn']} "
        f"CRM_USER={cfg['user']} "
        f"CRM_PASSWORD={cfg['password']} "
    )
    # stdin → perl - ; не пишем пароль в argv скрипта на диске
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        ssh_host,
        f"{env_prefix}perl -CSD",
    ]
    proc = subprocess.run(
        cmd,
        input=_PERL_FETCH.encode("utf-8"),
        capture_output=True,
        timeout=180,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or b"").decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"CRM SSH fetch failed (host={ssh_host}): {err}")

    text = proc.stdout.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    by_code: Dict[str, dict] = {}
    for row in reader:
        # нормализуем ключи (isql/DBI могут отличаться регистром)
        rec = {k: row.get(k) for k in row}
        # единый регистр ожидаемых полей
        mapped = {
            "Code1C": rec.get("Code1C") or rec.get("code1c"),
            "Name": rec.get("Name") or rec.get("name"),
            "Phone": rec.get("Phone") or rec.get("phone"),
            "Web": rec.get("Web") or rec.get("web"),
            "PassportId": rec.get("PassportId") or rec.get("passportid"),
            "IssuedBy": rec.get("IssuedBy") or rec.get("issuedby"),
            "IssuedDate": rec.get("IssuedDate") or rec.get("issueddate"),
            "Registration": rec.get("Registration") or rec.get("registration"),
            "SubjectTypeId": rec.get("SubjectTypeId") or rec.get("subjecttypeid"),
            "IDNumber": rec.get("IDNumber") or rec.get("idnumber"),
            "KPP": rec.get("KPP") or rec.get("kpp"),
            "PrimaryContactName": rec.get("PrimaryContactName")
            or rec.get("primarycontactname"),
            "PrimaryContactJobTitle": rec.get("PrimaryContactJobTitle")
            or rec.get("primarycontactjobtitle"),
            "OwnerContactName": rec.get("OwnerContactName")
            or rec.get("ownercontactname"),
            "OwnerContactEmail": rec.get("OwnerContactEmail")
            or rec.get("ownercontactemail"),
        }
        norm = _normalize_account_row(mapped)
        if norm["Code1C"]:
            by_code[norm["Code1C"]] = norm
    return by_code


def fetch_crm_accounts() -> Dict[str, dict]:
    """
    Справочник Account по Code1C.
    1) Локальный ODBC (pyodbc), если доступен.
    2) Иначе CRM_SSH_HOST (vz3).
    """
    cfg = _crm_env()
    try:
        return fetch_accounts_odbc()
    except Exception as local_err:
        if not cfg["ssh_host"]:
            raise RuntimeError(
                "CRM ODBC недоступен локально и CRM_SSH_HOST не задан. "
                f"Ошибка ODBC: {local_err}"
            ) from local_err
        try:
            return fetch_accounts_ssh(cfg["ssh_host"])
        except Exception as ssh_err:
            raise RuntimeError(
                f"CRM: локальный ODBC ({local_err}); SSH {cfg['ssh_host']} ({ssh_err})"
            ) from ssh_err


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent
