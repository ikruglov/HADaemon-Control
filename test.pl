#!/usr/bin/env perl

use strict;
use warnings;
use HADaemon::Control;

my $dc = HADaemon::Control->new({
    name => 'test.pl',
    user => 'nobody',
    pid_dir => '/tmp/test',
    log_file => '/tmp/test.log',
    program => sub { sleep 10; },
});

exit $dc->run();
