#!/usr/bin/env perl

use strict;
use warnings;
use HADaemon::Control;

HADaemon::Control->new({
    name => 'test.pl',
    user => 'nobody',
    #use_main_stop_file => 1,
    pid_dir => '/tmp/test',
    #program => sub { print $ENV{HADC_lock_fd} . "\n"; sleep 10; },
    program => sub { sleep 10; },
    ipc_cl_options => {
        max_procs => 1,
        standby_max_procs => 2,
        retries => sub { 1 },
    },
})->run();
