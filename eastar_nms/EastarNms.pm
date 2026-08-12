package EastarNms;

use strict;
use warnings;
use Carp qw(croak);
use File::Basename qw(dirname);
use File::Spec;
use HTTP::Cookies;
use JSON;
use LWP::UserAgent;
use POSIX qw(strftime);

use Exporter qw(import);
our @EXPORT_OK = qw(
  load_config
  login_ua
  nms_update
  nms_updatetree
  utc_now_iso
  html_to_text
  parse_kbps
  parse_db
  json_out
);

sub utc_now_iso {
    return strftime('%Y-%m-%dT%H:%M:%SZ', gmtime());
}

sub _read_env_file {
    my ($path, $dest) = @_;
    return unless $path && -f $path;
    open my $fh, '<', $path or return;
    while (my $line = <$fh>) {
        chomp $line;
        $line =~ s/^\s+|\s+$//g;
        next if $line eq '' || $line =~ /^#/ || $line !~ /=/;
        my ($k, $v) = split /=/, $line, 2;
        $k =~ s/^\s+|\s+$//g;
        $v =~ s/^\s+|\s+$//g;
        $v =~ s/^['"]|['"]$//g;
        $dest->{$k} = $v if $k ne '' && !exists $dest->{$k};
    }
    close $fh;
}

sub load_config {
    my (%opt) = @_;
    my $here = $opt{here} // dirname(File::Spec->rel2abs($0));
    my %env = %ENV;
    _read_env_file(File::Spec->catfile($here, 'config.env'), \%env);
    _read_env_file(File::Spec->catfile($here, 'config.env.example'), \%env);

    my $url = $opt{nms_url} // $env{EASTAR_NMS_URL} // 'https://192.168.10.49';
    $url =~ s{/$}{};
    my $cfg = {
        nms_url  => $url,
        login    => $opt{login}    // $env{EASTAR_NMS_LOGIN}    // croak('Missing EASTAR_NMS_LOGIN'),
        password => $opt{password} // $env{EASTAR_NMS_PASSWORD} // croak('Missing EASTAR_NMS_PASSWORD'),
        net_id   => 0 + ($opt{net_id} // $env{EASTAR_NET_ID} // 1),
        filter   => $opt{filter} // $env{EASTAR_FILTER} // '',
        timeout  => 0 + ($env{EASTAR_TIMEOUT} // 20),
    };
    return $cfg;
}

sub login_ua {
    my ($cfg) = @_;
    my $ua = LWP::UserAgent->new(
        timeout      => $cfg->{timeout},
        cookie_jar   => HTTP::Cookies->new,
        max_redirect => 5,
        ssl_opts     => { verify_hostname => 0, SSL_verify_mode => 0 },
        agent        => 'eastar_nms-collector/1.0',
    );
    my $res = $ua->post(
        $cfg->{nms_url} . '/login/insert/',
        Content => [
            'login[login]'    => $cfg->{login},
            'login[password]' => $cfg->{password},
        ],
    );
    if (!$res->is_success && $res->code != 302) {
        croak('NMS login failed: ' . $res->status_line);
    }
    return $ua;
}

sub nms_update {
    my ($ua, $cfg, @items) = @_;
    my $payload = encode_json(\@items);
    my $res = $ua->post(
        $cfg->{nms_url} . '/update/',
        Content => { req => $payload },
    );
    croak('NMS /update/ failed: ' . $res->status_line) unless $res->is_success;
    return $res->decoded_content // $res->content // '';
}

sub nms_updatetree {
    my ($ua, $cfg) = @_;
    my $res = $ua->post(
        $cfg->{nms_url} . '/updatetree/',
        Content => { checksum_state => '', checksum => '' },
    );
    croak('NMS /updatetree/ failed: ' . $res->status_line) unless $res->is_success;
    my $raw = $res->decoded_content // $res->content // '{}';
    return decode_json($raw);
}

sub html_to_text {
    my ($html) = @_;
    $html //= '';
    $html =~ s/<br\s*\/?>/\n/gi;
    $html =~ s/<[^>]+>/ /g;
    $html =~ s/&nbsp;/ /g;
    $html =~ s/&amp;/&/g;
    $html =~ s/&lt;/</g;
    $html =~ s/&gt;/>/g;
    $html =~ s/\s+/ /g;
    $html =~ s/^\s+|\s+$//g;
    return $html;
}

sub parse_kbps {
    my ($text) = @_;
    return undef unless defined $text;
    return 0 + $1 if $text =~ /([0-9]+(?:\.[0-9]+)?)\s*kbps/i;
    return undef;
}

sub parse_db {
    my ($text) = @_;
    return undef unless defined $text;
    return 0 + $1 if $text =~ /([0-9]+(?:\.[0-9]+)?)\s*dB/i;
    return undef;
}

sub json_out {
    my ($data) = @_;
    my $json = JSON->new->canonical(1)->pretty(1)->utf8->encode($data);
    print $json;
    print "\n" unless $json =~ /\n\z/;
}

1;
