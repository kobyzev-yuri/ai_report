"""HTTP-сессия Eastar NMS на urllib (без requests и без Perl)."""

from __future__ import print_function

import http.cookiejar
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request


class NmsClient(object):
    def __init__(self, cfg):
        self.cfg = cfg
        context = ssl._create_unverified_context()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
            urllib.request.HTTPSHandler(context=context),
        )
        self._opener.addheaders = [("User-Agent", "eastar_nms-collector/1.0")]

    def _request(self, path, data=None, ok_below=300):
        url = self.cfg["nms_url"] + path
        payload = None if data is None else urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(url, data=payload)
        try:
            res = self._opener.open(req, timeout=self.cfg["timeout"])
        except urllib.error.HTTPError as exc:
            raise SystemExit("NMS %s failed: %s %s" % (path, exc.code, exc.reason))
        except urllib.error.URLError as exc:
            reason = exc.reason
            raise SystemExit(
                "NMS %s failed: %s. Для HTTP укажите EASTAR_NMS_URL=http://..."
                % (path, reason)
            )
        code = res.getcode()
        raw = res.read()
        charset = res.headers.get_content_charset() or "utf-8"
        if not (200 <= code < ok_below):
            raise SystemExit("NMS %s failed: %s" % (path, code))
        return raw.decode(charset, "replace")

    def login(self):
        self._request(
            "/login/insert/",
            data={
                "login[login]": self.cfg["login"],
                "login[password]": self.cfg["password"],
            },
            ok_below=400,
        )

    def select_net(self):
        net_id = int(self.cfg.get("net_id") or 0)
        if net_id <= 0:
            return ""
        return self._request("/net_usage/?net_id=%s" % net_id)

    def select_hub(self):
        net_id = int(self.cfg.get("net_id") or 0)
        if net_id <= 0:
            return ""
        return self._request("/hub_usage/?net_id=%s" % net_id)

    def update(self, *items):
        payload = json.dumps(list(items), separators=(",", ":"), ensure_ascii=False)
        return self._request("/update/", data={"req": payload})

    def updatetree(self):
        raw = self._request("/updatetree/", data={"checksum_state": "", "checksum": ""})
        return json.loads(raw or "{}")
