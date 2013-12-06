package HADaemon::Control;

use strict;
use warnings;

use POSIX;
use File::Spec;
use File::Spec::Functions;
use File::Basename;
use File::Path qw(make_path);
use Scalar::Util qw(weaken);
use IPC::ConcurrencyLimit::WithStandby;

# Accessor building
my @accessors = qw(
    pid_dir quiet color_map name kill_timeout program program_args
    stdout_file stderr_file umask directory ipc_cl_options
    standby_stop_file
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
        kill_timeout  => 1,
    }, $class;

    foreach my $accessor (@accessors) {
        if (exists $args->{$accessor}) {
            $self->{$accessor} = delete $args->{$accessor};
        }
    }

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

    $self->_create_dir($self->pid_dir);

    $self->standby_stop_file
        or $self->standby_stop_file(catfile($self->pid_dir, 'standby-stop-file'));

    $self->{ipc_cl_options}->{path}
        or $self->{ipc_cl_options}->{path} = catdir($self->pid_dir, 'lock');
    $self->{ipc_cl_options}->{standby_path}
        or $self->{ipc_cl_options}->{standby_path} = catdir($self->pid_dir, 'lock-standby');

    my $called_with = $ARGV[0] // '';
    $called_with =~ s/^[-]+//g;

    my $allowed_actions = join('|', $self->_all_actions());
    $called_with
        or die "Must be called with an action: [$allowed_actions]\n";

    my $action = "do_$called_with";
    $self->can($action)
        and exit($self->$action() // 0);

    die "Error: unknown action $called_with. [$allowed_actions]\n";
}

#####################################
# commands
#####################################
sub do_start {
    my ($self) = @_;

    $self->_unlink_file($self->standby_stop_file);

    if ($self->_fork_mains() && $self->_fork_standbys()) {
        $self->pretty_print('starting main + standby processes', 'OK');
        return 0;
    }

    $self->pretty_print('starting main + standby processes', 'Failed', 'red');
    $self->do_status();
    return 1;
}

sub do_stop {
    my ($self) = @_;

    $self->_write_file($self->standby_stop_file);

    foreach my $type (reverse $self->_expected_processes()) {
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

    my @expected_main = $self->_expected_main_processes();
    my @expected_standby = $self->_expected_standby_processes();
    my %old_main_pids = map { $_ => $self->_pid_of_process_type($_) } @expected_main;

    # stoping standby
    $self->_write_file($self->standby_stop_file);

    foreach my $type (@expected_standby) {
        my $pid = $self->_pid_of_process_type($type);
        if ($pid && $self->_kill_pid($pid)) {
            $self->_unlink_file($self->_build_pid_file($type));
        }
    }

    if ($self->_standby_running()) {
        $self->pretty_print('stopping standby processes', 'Failed', 'red');
        warn "all standby processes should be stopped at this moment. Can't move forward\n";
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

    # restarting main
    foreach my $type (@expected_main) {
        my $pid = $self->_pid_of_process_type($type);
        if (!$pid || $self->_kill_pid($pid)) {
            $self->_fork_mains();
        }
    }

    # making sure the mains were restarted
    my $main_running = 0;
    foreach my $type (keys %old_main_pids) {
        my $old_pid = $old_main_pids{$type};
        my $new_pid = $self->_pid_of_process_type($type);
        next unless $new_pid;

        $main_running++;
        if ($old_pid && $old_pid == $new_pid) {
            $self->pretty_print($type, 'Failed to restart', 'red');
        }
    }

    if ($main_running != scalar @expected_main) {
        $self->pretty_print('restarting main', 'Failed', 'red');
        warn "all main processes should be running at this moment\n";
        return 1;
    }

    $self->pretty_print('restarting main processes', 'OK');

    # starting standbys again
    if (!$self->_fork_standbys()) {
        $self->pretty_print('starting standby', 'Failed', 'red');
        warn "all standby processes should be running at this moment. Can't move forward\n";
        return 1;
    }

    $self->pretty_print('starting standby processes', 'OK');
    return 0;
}

sub do_hard_restart {
    my ($self) = @_;
    $self->do_stop();
    return $self->do_start();
}

sub do_status {
    my ($self) = @_;
    foreach my $type ($self->_expected_processes()) {
        if ($self->_pid_of_process_type($type)) {
            $self->pretty_print("$type status", 'Running');
        } else {
            $self->pretty_print("$type status", 'Not Running', 'red');
        }
    }
}

sub do_fork {
    my ($self) = @_;

    $self->_fork(); # always spawn at least one new process
    $self->_fork_mains();
    $self->_fork_standbys();

    return 0;
}

#####################################
# routines to work with processes
#####################################
sub _fork_mains {
    my ($self) = @_;
    my $expected_main = $self->_expected_main_processes();

    for (1..10) {
        my $to_start = $expected_main - $self->_main_running();
        last unless $to_start;

        $self->_fork() foreach (1 .. $to_start);
        sleep($self->_sleep_interval);
    }

    return $self->_main_running() == $expected_main;
}

sub _fork_standbys {
    my ($self) = @_;
    my $expected_standby = $self->_expected_standby_processes();

    for (1..10) {
        my $to_start = $expected_standby - $self->_standby_running();
        last unless $to_start;

        $self->_fork() foreach (1 .. $to_start);
        sleep($self->_sleep_interval);
    }

    return $self->_standby_running() == $expected_standby;
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
    my $res = $pid && $pid > 1 ? kill(0, $pid) : 0;
    $self->trace("pid $pid is " . ($res ? 'running' : 'not running'));
    return $res;
}

# rename is pid_of_process_type
sub _pid_of_process_type {
    my ($self, $type) = @_;
    my $pidfile = $self->_build_pid_file($type);
    my $pid = $self->_read_file($pidfile);
    return $pid && $self->_pid_running($pid) ? $pid : undef;
}

sub _kill_pid {
    my ($self, $pid) = @_;

    foreach my $signal (qw(TERM TERM INT KILL)) {
        $self->trace("Sending $signal signal to pid $pid...");
        kill($signal, $pid);

        my $tries = $self->kill_timeout // 1;
        while ($tries-- && $self->_pid_running($pid)) {
            sleep 1;
        }

        return 1 if not $self->_pid_running($pid);
    }

    return 0;
}

#####################################
# forking functions
#####################################
sub _fork {
    my ($self) = @_;
    $self->trace("_double_fork()");

    my $pid = fork();
    $pid and $self->trace("forked $pid");

    if ($pid == 0) { # Child, launch the process here
        POSIX::setsid(); # Become the process leader

        my $pid2 = fork();
        $pid2 and $self->trace("forked $pid2");

        if ($pid2 == 0) { # Our double fork.
            # close all file handlers
            my $max_fd = POSIX::sysconf( &POSIX::_SC_OPEN_MAX );
            $max_fd = 64 if !defined $max_fd or $max_fd < 0;
            POSIX::close($_) foreach (3 .. $max_fd);

            # reopening STDIN and redirecting STDOUT and STDERR
            open(STDIN, "<", File::Spec->devnull);
            $self->_redirect_filehandles();

            if ($self->umask) {
                umask($self->umask);
                $self->trace("umask(" . $self->umask . ")");
            }

            if ($self->directory) {
                chdir($self->directory);
                $self->trace("chdir(" . $self->directory . ")");
            }

            # TODO add eval
            my $res = $self->_launch_program();
            exit($res // 0);
        } elsif (not defined $pid2) {
            warn "Cannot fork: $!";
            POSIX::_exit(1);
        } else {
            POSIX::_exit(0);
        }
    } elsif (not defined $pid) { # We couldn't fork =(
        die "Cannot fork: $!";
    }

    # Wait until first kid terminates
    $self->trace("waitpid()");
    waitpid($pid, 0) == $pid or die "waitpid() failed: $!";
    return POSIX::WIFEXITED(${^CHILD_ERROR_NATIVE})
           ? POSIX::WEXITSTATUS(${^CHILD_ERROR_NATIVE})
           : 1;
}

sub _redirect_filehandles {
    my ($self) = @_;

    if ($self->stdout_file) {
        my $file = $self->stdout_file;
        $file = $file eq '/dev/null' ? File::Spec->devnull : $file;
        open(STDOUT, '>>', $file)
            or die "Failed to open STDOUT to $file: $!";
        $self->trace("STDOUT redirected to $file");
    }

    if ($self->stderr_file) {
        my $file = $self->stderr_file;
        $file = $file eq '/dev/null' ? File::Spec->devnull : $file;
        open(STDERR, '>>', $file)
            or die "Failed to open STDERR to $file: $!";
        $self->trace("STDERR redirected to $file");
    }
}

sub _launch_program {
    my ($self) = @_;
    $self->trace("_launch_program()");

    return if -f $self->standby_stop_file;

    my $pid_file = $self->_build_pid_file("unknown-$$");
    $self->_write_file($pid_file, $$);
    $self->{pid_file} = $pid_file;

    my $ipc = IPC::ConcurrencyLimit::WithStandby->new(%{ $self->ipc_cl_options });

    # have to duplicate some logic from IPC::CL:WS
    my $retries_classback = $ipc->{retries};
    if (ref $retries_classback ne 'CODE') {
        my $max_retries = $retries_classback;
        $retries_classback = sub { return $_[0] != $max_retries + 1 };
    }

    my $ipc_weak = $ipc;
    weaken($ipc_weak);

    $ipc->{retries} = sub {
        if ($_[0] == 1) { # run code on first attempt
            my $id = $ipc->{standby_lock}->lock_id();
            $self->trace("acquired standby lock $id");

            # adjusting name of pidfile
            my $pid_file = $self->_build_pid_file("standby-$id");
            $self->_rename_file($self->{pid_file}, $pid_file);
            $self->{pid_file} = $pid_file;
        }

        return 0 if -f $self->standby_stop_file;
        return $retries_classback->(@_);
    };

    my $id = $ipc->get_lock();
    if (not $id) {
        $self->_unlink_file($self->{pid_file});
        $self->trace('failed to acquire both locks');
        return 1; # TODO call callback or maybe pass it $self->program
    }

    $self->trace("acquired main lock id: " . $ipc->lock_id());
    
    # now pid file should be 'main-$id'
    $pid_file = $self->_build_pid_file("main-$id");
    $self->_rename_file($self->{pid_file}, $pid_file);
    $self->{pid_file} = $pid_file;

    return 0 if -f $self->standby_stop_file;

    my @args = @{ $self->program_args // [] };
    my $res = $self->program->($self, @args);
    return $res // 0;
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

    open(my $fh, "<", $file) or die "Failed to read $file: $!";
    my $content = do { local $/; <$fh> };
    close($fh);

    $self->trace("read '$content' from file ($file)");
    return $content;
}

sub _write_file {
    my ($self, $file, $content) = @_;
    $content //= '';

    open(my $fh, ">", $file) or die "Failed to write $file: $!";
    print $fh $content;
    close($fh);

    $self->trace("wrote '$content' to file ($file)");
}

sub _rename_file {
    my ($self, $old_file, $new_file) = @_;
    rename($old_file, $new_file) and $self->trace("rename pid file ($old_file) to ($new_file)");
}

sub _unlink_file {
    my ($self, $file) = @_;
    unlink($file) and $self->trace("unlink file ($file)");
}

sub _create_dir {
    my ($self, $dir) = @_;
    if (-d $dir) {
        $self->trace("Dir exists ($dir) - no need to create");
    } else {
        make_path($dir, { error => \my $errors });
        @$errors and die "error TODO"; # TODO
        $self->trace("Created dir ($dir)");
    }
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

sub trace {
    my ($self, $message) = @_;
    return unless $ENV{DC_TRACE};

    print "$$ [TRACE] $message\n" if $ENV{DC_TRACE} == 1;
    print STDERR "$$ [TRACE] $message\n" if $ENV{DC_TRACE} == 2;
}

sub _all_actions {
    my ($self) = @_; 
    no strict 'refs';
    return map { m/^do_(.+)/ ? $1 : () } keys %{ ref($self) . '::' };
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

sub _expected_processes {
    my ($self) = @_;
    my @expected = (
        $self->_expected_main_processes(),
        $self->_expected_standby_processes(),
    );
    return wantarray ? @expected : scalar @expected;
}

sub _sleep_interval {
    return int(shift->{ipc_cl_options}->{interval} // 0) + 3;
}

1;
