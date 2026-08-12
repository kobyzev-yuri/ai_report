#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use Getopt::Long qw(GetOptions);
use EastarNms qw(
  load_config login_ua nms_update nms_updatetree utc_now_iso html_to_text parse_kbps json_out
);

my ($nms_url, $login, $password, $net_id, $filter, $help);
GetOptions(
    'nms-url=s'  => \$nms_url,
    'login=s'    => \$login,
    'password=s' => \$password,
    'net-id=i'   => \$net_id,
    'filter=s'   => \$filter,
    'help|h'     => \$help,
) or exit 2;

if ($help) {
    print "Usage: $0 [--filter KEY] [--nms-url URL] [--login USER] [--password PASS] [--net-id N]\n";
    exit 0;
}

my $cfg = load_config(
    nms_url  => $nms_url,
    login    => $login,
    password => $password,
    net_id   => $net_id,
    filter   => $filter,
);
my $key = $cfg->{filter} // '';
$key =~ s/^\s+|\s+$//g;

my $ua = login_ua($cfg);
my $tree = nms_updatetree($ua, $cfg);
my @controllers = @{ $tree->{controllers} // [] };

@controllers = grep {
    (!defined $_->{netid} || $_->{netid} == $cfg->{net_id})
} @controllers;

if ($key ne '') {
    my $lk = lc $key;
    @controllers = grep { index(lc($_->{n} // ''), $lk) >= 0 } @controllers;
}

my @out;
for my $c (@controllers) {
    my $cid = $c->{cid} // next;
    my $name = $c->{n} // '';
    my $html = nms_update(
        $ua, $cfg,
        { what => 'widget', datasrc => 'WidgetControllerStatus:' . $cid },
    );
    my $text = html_to_text($html);
    my ($tx, $rx);
    $tx = 0 + $1 if $text =~ /\bTX:\s*([0-9]+(?:\.[0-9]+)?)\s*kbps/i;
    $rx = 0 + $1 if $text =~ /\bRX:\s*([0-9]+(?:\.[0-9]+)?)\s*kbps/i;
    $tx = parse_kbps($1) if !defined $tx && $text =~ /TX\s+traffic\s*([0-9.]+\s*kbps)/i;
    $rx = parse_kbps($1) if !defined $rx && $text =~ /RX\s+traffic\s*([0-9.]+\s*kbps)/i;

    push @out, {
        name      => $name,
        cid       => 0 + $cid,
        tx_kbit_s => defined $tx ? 0 + $tx : undef,
        rx_kbit_s => defined $rx ? 0 + $rx : undef,
    };
}

json_out({
    source      => 'hub_usage',
    net_id      => $cfg->{net_id},
    filter      => $key,
    ts          => utc_now_iso(),
    stub        => JSON::false,
    controllers => \@out,
});
