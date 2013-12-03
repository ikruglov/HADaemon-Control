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
    stdout_file stderr_file umask directory limit_options
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
    $self->program && ref $self->program eq 'CODE'
        or die "Error: program must be defined and must be coderef";
    $self->name
        or die "Error: name must be defined";
    $self->pid_dir
        or die "Error: pid_dir must be defined";

    my $called_with = $ARGV[0] // '';
    $called_with =~ s/^[-]+//g;

    my $allowed_actions = join('|', $self->_all_actions());
    $called_with
        or die "Must be called with an action: [$allowed_actions]";

    my $action = "do_$called_with";
    $self->can($action)
        and exit($self->$action() // 0);

    die "Error: unknown action $called_with. [$allowed_actions]";
}

#####################################
# commands
#####################################
sub do_start {
    my ($self) = @_;
    my $exit_code = 0;

    my $pid_dir = $self->pid_dir;
    $self->_create_dir($pid_dir);

    my $interval = $self->{limit}->{interval} // 1;
    $interval = ($interval > 1 ? $interval : 1);

    FORK:
    for (1 .. $self->_num_of_main + $self->_num_of_standby) {
        my $pid = $self->_fork();
        if (not $pid || !$self->_pid_running($pid)) {
            $self->pretty_print('', 'Failed to start', 'red');
            $exit_code = 1;
            next;
        }

        for (1..2) {
            foreach my $type ($self->_expected_process_types()) {
                my $pidfile = $self->_build_pid_file($type);
                my $test_pid = $self->_read_pid_file($pidfile);
                if ($test_pid && $test_pid eq $pid) {
                    $self->pretty_print($type, 'Started');
                    next FORK;
                }
            }

            sleep($interval);
        }

        $self->pretty_print('', 'Unknown', 'red');
        print "Process started but failed to detect its type. Probably another instance is running or not enough permissions\n";
        $exit_code = 1;
    }

    return $exit_code;
}

sub do_status {
    my ($self) = @_;
    foreach my $type ($self->_expected_process_types()) {
        if ($self->_process_type_is_running($type)) {
            $self->pretty_print($type, 'Running');
        } else {
            $self->pretty_print($type, 'Not Running', 'red');
        }
    }
}

sub do_stop {
    my ($self) = @_;
    my $exit_code = 0;

    # start killing processes from standby

    NAME:
    foreach my $type ($self->_expected_process_types('reversed')) {
        my $pidfile = $self->_build_pid_file($type);
        my $pid = $self->_read_pid_file($pidfile);

        if ($pid && $self->_pid_running($pid)) {
            if ($self->_kill_pid($pid)) {
                $self->pretty_print($type, 'Stopped');
            } else {
                $self->pretty_print($type, 'Failed to stop', 'red');
                $exit_code = 1;
                next NAME;
            }

            my $npid = $self->_read_pid_file($pidfile);
            if ($npid && $npid == $pid) {
                $self->_unlink_pid_file($pidfile);
            }
        } else {
            $self->pretty_print($type, 'Not Running', 'red');
            $self->_unlink_pid_file($pidfile);
        }
    }

    return $exit_code;
}

sub do_restart {
    my ($self) = @_;
    $self->do_stop();
    return $self->do_start();
}

sub do_spawn {
    my ($self) = @_;
    foreach my $type ($self->_expected_process_types()) {
        $self->_process_type_is_running($type)
            or $self->_fork();
    }

    return 0;
}

#####################################
# routines to work with processes
#####################################
sub _pid_running {
    my ($self, $pid) = @_;
    my $res = $pid && $pid > 1 ? kill(0, $pid) : 0;
    $self->trace("pid $pid is " . ($res ? 'running' : 'not running'));
    return $res;
}

sub _process_type_is_running {
    my ($self, $type) = @_;

    my $pidfile = $self->_build_pid_file($type);
    my $pid = $self->_read_pid_file($pidfile);
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
    my $real_pid = ''; #pid of second fork
    $self->trace("_double_fork()");

    my ($pread, $pwrite);
    pipe($pread, $pwrite) or die "Cannot open a pipe: $!";

    my $pid = fork();
    $pid and $self->trace("forked $pid");

    if ($pid == 0) { # Child, launch the process here
        close($pread); # Close reading end of pipe
        POSIX::setsid(); # Become the process leader

        my $pid2 = fork();
        $pid2 and $self->trace("forked $pid2");

        if ($pid2 == 0) { # Our double fork.
            # close all file handlers
            # XXX if remove this block please don't forget to close $pwrite
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
            # send pid2 to initial parent
            syswrite($pwrite, $pid2);
            POSIX::_exit(0);
        }
    } elsif (not defined $pid) { # We couldn't fork =(
        die "Cannot fork: $!";
    } else { # In the parent, $pid = child's PID, return it
        close($pwrite); # Close writting end of pipe

        $self->trace("sysread()");
        sysread($pread, $real_pid, 16); # Read answer, block here
        close($pread);

        $self->trace("waitpid()");
        waitpid($pid, 0); # Wait until first kid terminates
    }

    $self->trace("got real pid: $real_pid");
    return $real_pid;
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

    my $pid_file = $self->_build_pid_file("unknown-$$");
    $self->_write_pid_file($pid_file, $$);
    $self->{pid_file} = $pid_file;

    my $limit = IPC::ConcurrencyLimit::WithStandby->new(%{ $self->limit_options });

    # have to duplicate some logic from IPC::CL:WS
    my $retries_classback = $limit->{retries};
    if (ref $retries_classback ne 'CODE') {
        my $max_retries = $retries_classback;
        $retries_classback = sub { return $_[0] != $max_retries + 1 };
    }

    my $first_callback_run = 1;
    my $limit_weak = $limit;
    weaken($limit_weak);

    $limit->{retries} = sub {
        if ($first_callback_run) {
            $first_callback_run = 0;

            my $id = $limit_weak->{standby_lock}->lock_id();
            $self->trace("acquired standby lock $id");

            # adjusting name of pidfile
            my $pid_file = $self->_build_pid_file("standby-$id");
            $self->_rename_pid_file($self->{pid_file}, $pid_file);
            $self->{pid_file} = $pid_file;
        }

        return $retries_classback->(@_);
    };

    my $id = $limit->get_lock();
    if (not $id) {
        $self->_unlink_pid_file($self->{pid_file});
        $self->trace('failed to acquire both locks');
        return 1; # TODO call callback or maybe pass it $self->program
    }

    $self->trace("acquired main lock id: " . $limit->lock_id());
    
    # now pid file should be 'main-$id'
    $pid_file = $self->_build_pid_file("main-$id");
    $self->_rename_pid_file($self->{pid_file}, $pid_file);
    $self->{pid_file} = $pid_file;

    my @args = @{ $self->program_args // [] };
    my $res = $self->program->($self, @args);
    return $res // 0;
}

#####################################
# pid file and file routines
#####################################
sub _build_pid_file {
    my ($self, $type) = @_;
    return catfile($self->pid_dir, "$type.pid");
}

sub _read_pid_file {
    my ($self, $pid_file) = @_;

    my $pid;
    if (open(my $fh, "<", $pid_file)) {
        $pid = do { local $/; <$fh> };
        close($fh);
    }

    $self->trace("read pid " . ($pid // "'undef'") . " from pid file ($pid_file)");
    return $pid;
}

sub _write_pid_file {
    my ($self, $pid_file, $pid) = @_;

    open(my $sf, ">", $pid_file) or die "Failed to write $pid_file: $!";
    print $sf $pid;
    close($sf);

    $self->trace("wrote pid ($pid) to pid file ($pid_file)");
    return $self;
}

sub _rename_pid_file {
    my ($self, $old_pid_file, $new_pid_file) = @_;
    rename($old_pid_file, $new_pid_file) or die "Failed to rename pidfile: $!";
    $self->trace("rename pid file ($old_pid_file) to ($new_pid_file)");
    return $self;
}

sub _unlink_pid_file {
    my ($self, $pid_file) = @_;
    unlink($pid_file) and $self->trace("unlink pid file ($pid_file)");
    return $self;
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
    printf("%s %-40s %40s\n", $self->name, $process_type, "\033[$code" ."m[$message]\033[0m");
}

sub trace {
    my ($self, $message) = @_;
    return unless $ENV{DC_TRACE};

    print "[TRACE] $message\n" if $ENV{DC_TRACE} == 1;
    print STDERR "[TRACE] $message\n" if $ENV{DC_TRACE} == 2;
}

sub _num_of_main {
    return shift->{limit_options}->{max_procs} // 0;
}

sub _num_of_standby {
    return shift->{limit_options}->{standby_max_procs} // 0;
}

sub _all_actions {
    my ($self) = @_; 
    no strict 'refs';
    return map { m/^do_(.+)/ ? $1 : () } keys %{ ref($self) . '::' };
}

sub _expected_process_types {
    my ($self, $reversed) = @_;

    my @expected;
    push @expected, "main-$_" foreach (1..$self->_num_of_main);
    push @expected, "standby-$_" foreach (1..$self->_num_of_standby);

    return $reversed ? reverse @expected : @expected;
}

1;
