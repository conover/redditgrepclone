#! /usr/bin/perl -w

use strict;

my $start_time =  6 * 3600 + 52 * 60; # 6:52am
my $end_time   = 31 * 3600 + 13 * 60; # 7:13am the next day
my $avg_step   = .000003;

my $t = $start_time;
while($t <= $end_time) {
    if ($t < 86400) {
        print "Feb  10 ";
    } else {
        print "Feb 11 ";
    }
    my $h = $t % 86400 / 3600;
    my $m = $t %  3600 /   60;
    my $s = $t %    60;
    printf "%0.2d:%0.2d:%0.2d ", $h, $m, $s;
    print "blah " x (3 + rand(10));
    print "\n";
    $t += $avg_step * 0.9;
    $t += rand($avg_step * 0.2);
}