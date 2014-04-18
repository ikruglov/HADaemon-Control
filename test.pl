#!/usr/bin/env perl

use strict;
use warnings;
use HADaemon::Control;

my $dc = HADaemon::Control->new({
    name => 'test.pl',
    user => 'nobody',
    pid_dir => '/tmp/test',
    program => sub { sleep 10; },
    ipc_cl_options => {
        max_procs => 1,
        standby_max_procs => 2,
        retries => sub { 1 },
    },
});

exit $dc->run();
