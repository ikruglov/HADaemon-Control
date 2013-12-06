#!/usr/bin/env perl

use strict;
use warnings;
use HADaemon::Control;

HADaemon::Control->new({
    name => 'test.pl',
    pid_dir => '/tmp/test',
    program => sub { sleep 10 },
    ipc_cl_options => {
        max_procs => 1,
        standby_max_procs => 2,
        path => '/tmp/test/lock',
        standby_path => '/tmp/test/lock-standby',
        retries => sub { 1 },
    },
})->run();

