#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use Getopt::Long qw(GetOptions);
use EastarNms qw(
  load_config login_ua nms_update utc_now_iso html_to_text parse_kbps json_out
);

my ($nms_url, $login, $password, $net_id, $help);
GetOptions(
    'nms-url=s'  => \$nms_url,
    'login=s'    => \$login,
    'password=s' => \$password,
    'net-id=i'   => \$net_id,
    'help|h'     => \$help,
) or exit 2;

if ($help) {
    print "Usage: $0 [--nms-url URL] [--login USER] [--password PASS] [--net-id N]\n";
    exit 0;
}

my $cfg = load_config(
    nms_url  => $nms_url,
    login    => $login,
    password => $password,
    net_id   => $net_id,
);

my $ua = login_ua($cfg);
my $html = nms_update(
    $ua, $cfg,
    { what => 'widget', datasrc => 'WidgetNetworkStatus:' . $cfg->{net_id} },
);
my $text = html_to_text($html);

my %row;
# Example: Stations RX 3 / 5 0 3 0 dB 0 0 Station RX: 0.0 kbps
if ($text =~ /Stations\s+RX\s+(\d+)\s*\/\s*(\d+)\s+(\d+|-)\s+(\d+)\s+([0-9.]+)\s*dB.*?Station\s+RX:\s*([0-9.]+)\s*kbps/i) {
    $row{stations_enabled} = "$1 / $2";
    $row{stations_online}  = ($3 eq '-') ? 0 : 0 + $3;
    $row{stations_down}    = 0 + $4;
    $row{stations_cn_db}   = 0 + $5;
    $row{stations_rx_kbit_s} = 0 + $6;
}
if ($text =~ /Controllers\s+RX\s+(\d+)\s*\/\s*(\d+)\s+(\d+|-)\s+(\d+)\s+([0-9.]+)\s*dB.*?Controllers\s+RX:\s*([0-9.]+)\s*kbps/i) {
    $row{controllers_enabled} = "$1 / $2";
    $row{controllers_online}  = ($3 eq '-') ? 0 : 0 + $3;
    $row{controllers_down}    = 0 + $4;
    $row{controllers_cn_db}   = 0 + $5;
    $row{controllers_rx_kbit_s} = 0 + $6;
}

$row{stations_rx_kbit_s} = parse_kbps($1)
  if !defined $row{stations_rx_kbit_s} && $text =~ /Station\s+RX:\s*([0-9.]+\s*kbps)/i;
$row{controllers_rx_kbit_s} = parse_kbps($1)
  if !defined $row{controllers_rx_kbit_s} && $text =~ /Controllers\s+RX:\s*([0-9.]+\s*kbps)/i;

json_out({
    source => 'net_usage',
    net_id => 0 + $cfg->{net_id},
    ts     => utc_now_iso(),
    stub   => JSON::false,
    stations_enabled   => $row{stations_enabled},
    stations_online    => $row{stations_online},
    stations_down      => $row{stations_down},
    stations_cn_db     => $row{stations_cn_db},
    stations_rx_kbit_s => $row{stations_rx_kbit_s},
    controllers_enabled   => $row{controllers_enabled},
    controllers_online    => $row{controllers_online},
    controllers_down      => $row{controllers_down},
    controllers_cn_db     => $row{controllers_cn_db},
    controllers_rx_kbit_s => $row{controllers_rx_kbit_s},
    # Compatibility aliases with earlier Python stub names
    inroute_cn_db     => $row{stations_cn_db},
    network_rx_kbit_s => $row{stations_rx_kbit_s} // $row{controllers_rx_kbit_s},
});
