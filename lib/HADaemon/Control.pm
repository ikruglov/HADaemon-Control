package HADaemon::Control;

use strict;
use warnings;

use POSIX ();
use File::Basename;
use File::Spec::Functions;
use File::Path qw(make_path);
use Scalar::Util qw(weaken);
use IPC::ConcurrencyLimit::WithStandby;

# Accessor building
my @accessors = qw(
    pid_dir quiet color_map name kill_timeout program program_args
    stdout_file stderr_file umask directory ipc_cl_options
    standby_stop_file uid gid log_file process_name_change
    called_with
);

foreach my $method (@accessors) {
    no strict 'refs';
    *$method = sub {
        my $self = shift;
        $self->{$method} = shift if @_;
        return $self->{$method};
    }
}

sub new {
    my ($class, $args) = @_;

    my $self = bless {
        color_map     => { red => 31, green => 32 },
        quiet         => 0,
    }, $class;

    foreach my $accessor (@accessors) {
        if (exists $args->{$accessor}) {
            $self->{$accessor} = delete $args->{$accessor};
        }
    }

    $self->user(delete $args->{user}) if exists $args->{user};
    $self->group(delete $args->{group}) if exists $args->{group};

    die "Unknown arguments to the constructor: " . join(' ' , keys %$args)
        if keys %$args;

    return $self;
}

sub run {
    my ($self) = @_;

    # Error Checking.
    $self->ipc_cl_options
        or die "Error: ipc_cl_options must be defined\n";
    $self->program && ref $self->program eq 'CODE'
        or die "Error: program must be defined and must be coderef\n";
    $self->name
        or die "Error: name must be defined\n";
    $self->pid_dir
        or die "Error: pid_dir must be defined\n";

    defined($self->kill_timeout)
        or $self->kill_timeout(1);

    $self->standby_stop_file
        or $self->standby_stop_file(catfile($self->pid_dir, 'standby-stop-file'));

    $self->{ipc_cl_options}->{standby_max_procs}
        and not defined $self->{ipc_cl_options}->{retries}
            and warn "ipc_cl_options: 'standby_max_procs' defined but 'retries' not";

    ($self->{ipc_cl_options}->{type} //= 'Flock') eq 'Flock'
        or die "can work only with Flock backend\n";

    $self->{ipc_cl_options}->{path}
        or $self->{ipc_cl_options}->{path} = catdir($self->pid_dir, 'lock');
    $self->{ipc_cl_options}->{standby_path}
        or $self->{ipc_cl_options}->{standby_path} = catdir($self->pid_dir, 'lock-standby');
    $self->{process_name_change}
        and $self->{ipc_cl_options}->{process_name_change} = 1;

    if ($self->uid) {
        my @uiddata = getpwuid($self->uid);
        @uiddata or die "failed to get info about " . $self->uid . "\n";

        if (!$self->gid) {
            $self->gid($uiddata[3]);
            $self->trace("Implicit GID => " . $uiddata[3]);
        }

        $self->user
            or $self->{user} = $uiddata[0];

        $self->{user_home_dir} = $uiddata[7];
    }

    if ($self->log_file) {
        open(my $fh, '>>', $self->log_file)
            or die "failed to open logfile '" . $self->log_file . "': $!\n";

        $self->{log_fh} = $fh;
        chown $self->uid, $self->gid, $self->{log_fh} if $self->uid;
    }

    my $called_with = $ARGV[0] // '';
    $called_with =~ s/^[-]+//g;

    my $allowed_actions = join('|', reverse sort $self->_all_actions());
    $called_with
        or die "Must be called with an action: [$allowed_actions]\n";

    my $action = "do_$called_with";
    if ($self->can($action)) {
        $self->{called_with} = $called_with;
        $self->_create_dir($self->pid_dir);
        return $self->$action() // 0;
    }

    die "Error: unknown action $called_with. [$allowed_actions]\n";
}

#####################################
# commands
#####################################
sub do_start {
    my ($self) = @_;
    $self->info('do_start()');

    my $expected_main = $self->_expected_main_processes();
    my $expected_standby = $self->_expected_standby_processes();
    if (   $self->_main_running() == $expected_main
        && $self->_standby_running() == $expected_standby)
    {
        $self->pretty_print('starting main + standby processes', 'Already Running');
        $self->trace("do_start(): all processes are already running");
        return 0;
    }

    $self->_unlink_file($self->standby_stop_file);

    if ($self->_fork_mains() && $self->_fork_standbys()) {
        $self->pretty_print('starting main + standby processes', 'OK');
        return 0;
    }

    $self->pretty_print('starting main + standby processes', 'Failed', 'red');
    $self->do_status();
    $self->detect_stolen_lock();

    return 1;
}

sub do_stop {
    my ($self) = @_;
    $self->info('do_stop()');

    if (!$self->_main_running() && !$self->_standby_running()) {
        $self->pretty_print('stopping main + standby processes', 'Not Running', 'red');
        $self->trace("do_stop(): all processes are not running");
        return 0;
    }

    $self->_write_file($self->standby_stop_file);
    $self->_wait_standbys_to_complete();

    foreach my $type ($self->_expected_main_processes()) {
        my $pid = $self->_pid_of_process_type($type);
        if ($pid && $self->_kill_pid($pid)) {
            $self->_unlink_file($self->_build_pid_file($type));
        }
    }

    if ($self->_main_running() == 0 && $self->_standby_running() == 0) {
        $self->pretty_print('stopping main + standby processes', 'OK');
        return 0;
    }

    $self->pretty_print('stopping main + standby processes', 'Failed', 'red');
    $self->do_status();
    return 1;
}

sub do_restart {
    my ($self) = @_;
    $self->info('do_restart()');

    # shortcut
    if (!$self->_main_running() && !$self->_standby_running()) {
        return $self->do_start();
    }

    # stoping standby
    $self->_write_file($self->standby_stop_file);
    if (not $self->_wait_standbys_to_complete()) {
        $self->pretty_print('stopping standby processes', 'Failed', 'red');
        $self->warn("all standby processes should be stopped at this moment. Can't move forward");
        return 1;
    }

    $self->pretty_print('stopping standby processes', 'OK');

    # starting standby
    $self->_unlink_file($self->standby_stop_file);

    if (!$self->_fork_standbys()) {
        $self->pretty_print('starting standby', 'Failed', 'red');
        warn "all standby processes should be running at this moment. Can't move forward\n";
        return 1;
    }

    $self->pretty_print('starting standby processes', 'OK');

    # restarting mains and standbys
    foreach my $type ($self->_expected_main_processes()) {
        $self->_restart_main($type)
            or $self->pretty_print($type, 'Failed to restart', 'red');
    }

    # starting mains
    if (!$self->_fork_mains() || !$self->_fork_standbys()) {
        $self->pretty_print('restarting main + standby processes', 'Failed', 'red');
        $self->warn("all main + standby processes should be running at this moment");

        $self->do_status();
        $self->detect_stolen_lock();
        return 1;
    }

    $self->pretty_print('restarting main processes', 'OK');
    return 0;
}

sub do_hard_restart {
    my ($self) = @_;
    $self->info('do_hard_restart()');

    $self->do_stop();
    return $self->do_start();
}

sub do_status {
    my ($self) = @_;
    $self->info('do_status()');

    foreach my $type ($self->_expected_main_processes(), $self->_expected_standby_processes()) {
        if ($self->_pid_of_process_type($type)) {
            $self->pretty_print("$type status", 'Running');
        } else {
            $self->pretty_print("$type status", 'Not Running', 'red');
        }
    }
}

sub do_fork {
    my ($self) = @_;
    $self->info('do_fork()');
    return 1 if $self->_check_stop_file();

    $self->_fork_mains();
    $self->_fork_standbys();
    return 0;
}

sub do_reload {
    my ($self) = @_;
    $self->info('do_reload()');

    foreach my $type ($self->_expected_main_processes()) {
        my $pid = $self->_pid_of_process_type($type);
        if ($pid) {
            $self->_kill_or_die('HUP', $pid);
            $self->pretty_print($type, 'Reloaded');
        } else {
            $self->pretty_print("$type status", 'Not Running', 'red');
        }
    }
}

#####################################
# routines to work with processes
#####################################
sub _fork_mains {
    my ($self) = @_;
    my $expected_main = $self->_expected_main_processes();

    for (1..3) {
        my $to_start = $expected_main - $self->_main_running();
        $self->_fork() foreach (1 .. $to_start);

        for (1 .. $self->_standby_timeout) {
            return 1 if $self->_main_running() == $expected_main;
            sleep(1);
        }
    }

    return 0;
}

sub _fork_standbys {
    my ($self) = @_;
    my $expected_standby = $self->_expected_standby_processes();

    for (1..3) {
        my $to_start = $expected_standby - $self->_standby_running();
        $self->_fork() foreach (1 .. $to_start);

        for (1 .. $self->_standby_timeout) {
            return 1 if $self->_standby_running() == $expected_standby;
            sleep(1);
        }
    }

    return 0;
}

sub _main_running {
    my ($self) = @_;
    my @running = grep { $self->_pid_of_process_type($_) } $self->_expected_main_processes();
    return wantarray ? @running : scalar @running;
}

sub _standby_running {
    my ($self) = @_;
    my @running = grep { $self->_pid_of_process_type($_) } $self->_expected_standby_processes();
    return wantarray ? @running : scalar @running;
}

sub _pid_running {
    my ($self, $pid) = @_;

    if (not $pid) {
        $self->trace("_pid_running: invalid pid"),
        return 0;
    }

    my $res = $self->_kill_or_die(0, $pid);
    $self->trace("pid $pid is " . ($res ? 'running' : 'not running'));
    return $res;
}

sub _pid_of_process_type {
    my ($self, $type) = @_;
    my $pidfile = $self->_build_pid_file($type);
    my $pid = $self->_read_file($pidfile);
    return $pid && $self->_pid_running($pid) ? $pid : undef;
}

sub _kill_pid {
    my ($self, $pid) = @_;
    $self->trace("_kill_pid(): $pid");

    foreach my $signal (qw(TERM TERM INT KILL)) {
        $self->trace("Sending $signal signal to pid $pid...");
        $self->_kill_or_die($signal, $pid);

        for (1 .. $self->kill_timeout) {
            if (not $self->_pid_running($pid)) {
                $self->trace("Successfully killed $pid");
                return 1;
            }

            sleep 1;
        }
    }

    $self->trace("Failed to kill $pid");
    return 0;
}

sub _restart_main {
    my ($self, $type) = @_;
    $self->trace("_restart_main(): $type");

    my $pid = $self->_pid_of_process_type($type);
    if (not $pid) {
        $self->trace("Main process $type is not running");
        return 1;
    }

    foreach my $signal (qw(TERM TERM INT KILL)) {
        $self->trace("Sending $signal signal to pid $pid...");
        $self->_kill_or_die($signal, $pid);

        # wait until pid change
        for (1 .. $self->kill_timeout) {
            my $new_pid = $self->_pid_of_process_type($type);
            if ($new_pid && $pid != $new_pid) {
                $self->trace("Successfully restarted main $type");
                return 1;
            }

            sleep 1;
        }
    }

    $self->trace("Failed to restart main $type");
    return 0;
}

sub _kill_or_die {
    my ($self, $signal, $pid) = @_;

    my $res = kill($signal, $pid);
    if (!$res && $! != POSIX::ESRCH) {
        # don't want to die if proccess simply doesn't exists
        my $msg = "failed to send signal to pid $pid: $!" . ($! == POSIX::EPERM ? ' (not enough permissions, probably should run as root)' : '');
        $self->warn($msg, 1);
        die "$msg\n";
    }

    return $res;
}

sub _wait_standbys_to_complete {
    my ($self) = @_;
    $self->trace('_wait_all_standbys_to_complete()');

    for (1 .. $self->_standby_timeout) {
        return 1 if $self->_standby_running() == 0;
        sleep(1);
    }

    return 0;
}

sub _fork {
    my ($self) = @_;
    $self->trace("_double_fork()");
    my $parent_pid = $$;

    my $pid = fork();
    $pid and $self->trace("forked $pid");

    if ($pid == 0) { # Child, launch the process here
        # Become session leader
        POSIX::setsid() or $self->warn("failed to setsid: $!");

        my $pid2 = fork();
        $pid2 and $self->trace("forked $pid2");

        if ($pid2 == 0) { # Our double fork.
            # close all file handlers but logging one
            my $log_fd = $self->{log_fh} ? fileno($self->{log_fh}) : -1;
            my $max_fd = POSIX::sysconf( &POSIX::_SC_OPEN_MAX );
            $max_fd = 64 if !defined $max_fd or $max_fd < 0;
            $log_fd != $_ and POSIX::close($_) foreach (0 .. $max_fd);

            # reopen stad descriptors
            $self->_open_std_filehandles();

            if ($self->gid) {
                $self->trace("setgid(" . $self->gid . ")");
                POSIX::setgid($self->gid) or $self->warn("failed to setgid: $!");
            }

            if ($self->uid) {
                $self->trace("setuid(" . $self->uid . ")");
                POSIX::setuid($self->uid) or $self->warn("failed to setuid: $!");

                $ENV{USER} = $self->{user};
                $ENV{HOME} = $self->{user_home_dir};
                $self->trace("\$ENV{USER} => " . $ENV{USER});
                $self->trace("\$ENV{HOME} => " . $ENV{HOME});
            }

            if ($self->umask) {
                umask($self->umask);
                $self->trace("umask(" . $self->umask . ")");
            }

            if ($self->directory) {
                chdir($self->directory);
                $self->trace("chdir(" . $self->directory . ")");
            }

            my $res = $self->_launch_program();
            exit($res // 0);
        } elsif (not defined $pid2) {
            $self->warn("cannot fork: $!");
            POSIX::_exit(1);
        } else {
            $self->info("parent process ($parent_pid) forked child ($pid2)");
            POSIX::_exit(0);
        }
    } elsif (not defined $pid) { # We couldn't fork =(
        $self->warn("cannot fork: $!");
    } else {
        # Wait until first kid terminates
        $self->trace("waitpid()");
        waitpid($pid, 0);
    }
}

sub _open_std_filehandles {
    my ($self) = @_;

    # reopening STDIN, STDOUT, STDERR
    open(STDIN, '<', '/dev/null') or die "Failed to open STDIN: $!";

    my $stdout = $self->stdout_file // '/dev/null';
    open(STDOUT, '>>', $stdout) or die "Failed to open STDOUT to $stdout: $!\n";
    $self->trace("STDOUT redirected to $stdout");

    my $stderr = $self->stderr_file // '/dev/null';
    open(STDERR, '>>', $stderr) or die "Failed to open STDERR to $stderr: $!\n";
    $self->trace("STDERR redirected to $stderr");
}

sub _launch_program {
    my ($self) = @_;
    $self->trace("_launch_program()");
    return if $self->_check_stop_file();

    $self->process_name_change
        and $0 = $self->name;

    my $pid_file = $self->_build_pid_file("unknown-$$");
    $self->_write_file($pid_file, $$);
    $self->{pid_file} = $pid_file;

    my $ipc = IPC::ConcurrencyLimit::WithStandby->new(%{ $self->ipc_cl_options });

    # have to duplicate this logic from IPC::CL:WS
    my $retries_classback = $ipc->{retries};
    if (ref $retries_classback ne 'CODE') {
        my $max_retries = $retries_classback;
        $retries_classback = sub { return $_[0] != $max_retries + 1 };
    }

    my $ipc_weak = $ipc;
    weaken($ipc_weak);

    $ipc->{retries} = sub {
        if ($_[0] == 1) { # run code on first attempt
            my $id = $ipc_weak->{standby_lock}->lock_id();
            $self->info("acquired standby lock $id");

            # adjusting name of pidfile
            my $pid_file = $self->_build_pid_file("standby-$id");
            $self->_rename_file($self->{pid_file}, $pid_file);
            $self->{pid_file} = $pid_file;
        }

        return 0 if $self->_check_stop_file();
        return $retries_classback->(@_);
    };

    my $id = $ipc->get_lock();
    if (not $id) {
        $self->_unlink_file($self->{pid_file});
        $self->info('failed to acquire both locks, exiting...');
        return 1;
    }

    $self->info("acquired main lock id: " . $ipc->lock_id());
    
    # now pid file should be 'main-$id'
    $pid_file = $self->_build_pid_file("main-$id");
    $self->_rename_file($self->{pid_file}, $pid_file);
    $self->{pid_file} = $pid_file;

    my $res = 0;
    if (not $self->_check_stop_file()) {
        my $lock_fd = $self->_main_lock_fd($ipc);
        $lock_fd and $ENV{HADC_lock_fd} = $lock_fd;
        $self->{log_fh} and close($self->{log_fh});

        my @args = @{ $self->program_args // [] };
        $res = $self->program->($self, @args);
    }

    $self->_unlink_file($self->{pid_file});
    return $res // 0;
}

sub _expected_main_processes {
    my ($self) = @_;
    my $num = $self->{ipc_cl_options}->{max_procs} // 0;
    my @expected = map { "main-$_" } ( 1 .. $num );
    return wantarray ? @expected : scalar @expected;
}

sub _expected_standby_processes {
    my ($self) = @_;
    my $num = $self->{ipc_cl_options}->{standby_max_procs} // 0;
    my @expected = map { "standby-$_" } ( 1 .. $num );
    return wantarray ? @expected : scalar @expected;
}

#####################################
# file routines
#####################################
sub _build_pid_file {
    my ($self, $type) = @_;
    return catfile($self->pid_dir, "$type.pid");
}

sub _read_file {
    my ($self, $file) = @_;
    return undef unless -f $file;

    open(my $fh, '<', $file) or die "failed to read $file: $!\n";
    my $content = do { local $/; <$fh> };
    close($fh);

    $self->trace("read '$content' from file ($file)");
    return $content;
}

sub _write_file {
    my ($self, $file, $content) = @_;
    $content //= '';

    open(my $fh, '>', $file) or die "failed to write $file: $!\n";
    print $fh $content;
    close($fh);

    $self->trace("wrote '$content' to file ($file)");
}

sub _rename_file {
    my ($self, $old_file, $new_file) = @_;
    rename($old_file, $new_file) or die "failed to rename '$old_file' to '$new_file': $!\n";
    $self->trace("rename pid file ($old_file) to ($new_file)");
}

sub _unlink_file {
    my ($self, $file) = @_;
    return unless -f $file;
    unlink($file) or die "failed to unlink file '$file': $!\n";
    $self->trace("unlink file ($file)");
}

sub _create_dir {
    my ($self, $dir) = @_;
    if (-d $dir) {
        $self->trace("Dir exists ($dir) - no need to create");
    } else {
        make_path($dir, { uid => $self->uid, group => $self->gid, error => \my $errors });
        @$errors and die "failed make_path: " . join(' ', map { keys $_, values $_ } @$errors) . "\n";
        $self->trace("Created dir ($dir)");
    }
}

sub _check_stop_file {
    my $self = shift;
    if (-f $self->standby_stop_file()) {
        $self->info('stop file detected');
        return 1;
    } else {
        return 0;
    }
}

#####################################
# uid/gid routines
#####################################
sub user {
    my ($self, $user) = @_;

    if ($user) {
        my $uid = getpwnam($user);
        die "Error: Couldn't get uid for non-existent user $user\n"
            unless defined $uid;

        $self->{uid} = $uid;
        $self->{user} = $user;
        $self->trace("Set UID => $uid");
    }

    return $self->{user};
}

sub group {
    my ($self, $group) = @_;

    if ($group) {
        my $gid = getgrnam($group);
        die "Error: Couldn't get gid for non-existent group $group\n"
            unless defined $gid;

        $self->{gid} = $gid;
        $self->{group} = $group;
        $self->trace("Set GID => $gid");
    }

    return $self->{group};
}

#####################################
# lock detection logic
#####################################
sub detect_stolen_lock {
    my ($self) = @_;
    $self->_main_running() != $self->_expected_main_processes() && $self->_standby_running() == $self->_expected_standby_processes()
        and $self->warn("one of main processes failed to acquire main lock, something is possibly holding it!!!");
}

sub _main_lock_fd {
    my ($self, $ipc) = @_;
    if (   exists $ipc->{main_lock}
        && exists $ipc->{main_lock}->{lock_obj}
        && exists $ipc->{main_lock}->{lock_obj}->{lock_fh})
    {
        my $fd = fileno($ipc->{main_lock}->{lock_obj}->{lock_fh});
        $self->trace("detected lock fd: $fd");
        return $fd;
    }

    $self->warn("failed to detect lock fd");
    return undef;
}

#####################################
# misc
#####################################
sub pretty_print {
    my ($self, $process_type, $message, $color) = @_;
    return if $self->quiet;

    $color //= "green"; # Green is no color.
    my $code = $self->color_map->{$color} //= 32; # Green is invalid.

    local $| = 1;
    $process_type =~ s/-/ #/;
    printf("%s: %-40s %40s\n", $self->name, $process_type, "\033[$code" ."m[$message]\033[0m");
}

sub info { $_[0]->_log('INFO', $_[1]); }
sub warn { $_[0]->_log('WARN', $_[1]); $_[2] or warn $_[1] . "\n"; }
sub trace { $ENV{DC_TRACE} and $_[0]->_log('TRACE', $_[1]); }

sub _log {
    my ($self, $level, $message) = @_;
    if ($self->{log_fh} && defined fileno($self->{log_fh})) {
        my $date = POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime(time()));
        printf { $self->{log_fh} } "[%s][%d][%s] %s\n", $date, $$, $level, $message;
        $self->{log_fh}->flush();
    }
}

sub _all_actions {
    my ($self) = @_; 
    no strict 'refs';
    return map { m/^do_(.+)/ ? $1 : () } keys %{ ref($self) . '::' };
}

sub _standby_timeout {
    return int(shift->{ipc_cl_options}->{interval} // 0) + 3;
}

1;

__END__

=encoding utf8

=head1 NAME

HADaemon::Control - Create init scripts for Perl high-available (HA) daemons

=head1 DESCRIPTION

HADaemon::Control provides a library for creating init scripts for HA daemons in perl.
The library takes idea and interface from Daemon::Control and combine them
with facilities of IPC::ConcurrencyLimit::WithStandby.

While creating the module I was aiming to achieve four goals:

=over 4

=item * starting/stopping/restarting HA daemon with one command

=item * have a clear way of getting status of daemon's processes

=item * minimize downtime during a restart

=item * ability to run module as a cronjob to fork new processes

=back

=head1 SYNOPSIS

=head1 CONSTRUCTOR

The constructor takes the following arguments.

=head2 name

The name of the program the daemon is controlling.  This will be used in
status messages "name [Started]"

=head2 program

This should be a coderef of actual programm to run.

    $daemon->program( sub { ... } );

=head2 program_args

This is an array ref of the arguments for the program.

=head1 METHODS

=head2 do_start

Is called when start is given as an argument.  Starts the forking and
exits. Called by:

    /usr/bin/my_program_launcher.pl start

=head2 do_stop

Is called when stop is given as an argument.  Stops the running program
if it can. Called by:

        /usr/bin/my_program_launcher.pl stop

=head2 do_restart

Is called when restart is given as an argument.  Calls do_stop and do_start.
Called by:

    /usr/bin/my_program_launcher.pl restart

=head2 do_reload

Is called when reload is given as an argument.  Sends a HUP signal to the
daemon.

    /usr/bin/my_program_launcher.pl reload

=head2 do_status

Is called when status is given as an argument.  Displays the status of the
program, basic on the PID file. Called by:

    /usr/bin/my_program_launcher.pl status

=head1 AUTHOR

Ivan Kruglov, C<ivan.kruglov@yahoo.com>

=head1 ACKNOWLEDGMENT

This module was inspired from module Daemon::Control.

This module was originally developed for Booking.com.
With approval from Booking.com, this module was generalized
and put on CPAN, for which the authors would like to express
their gratitude.

=head1 COPYRIGHT AND LICENSE

(C) 2013 Ivan Kruglov. All rights reserved.

This code is available under the same license as Perl version
5.8.1 or higher.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

=head2 AVAILABILITY

The most current version of HADaemon::Control can be found at L<https://github.com/ikruglov/HADaemon-Control>
