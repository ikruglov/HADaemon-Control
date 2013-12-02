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
    stdout_file stderr_file umask directory
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

    my $limit_options = delete $args->{limit_options};
    my $limit = IPC::ConcurrencyLimit::WithStandby->new(%$limit_options);

    my $self = bless {
        limit         => $limit,
        limit_options => $limit_options,
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

    $self->_create_dir($self->pid_dir);

    my $exit_code = 0;
    for (1 .. $self->_num_of_main + $self->_num_of_standby) {
        my $feedback = $self->_fork() // '';
        if ($feedback =~ m/^(?:main|standby)-\d+$/) {
            $self->pretty_print($feedback, 'Started');
        } else {
            $self->pretty_print('', 'Failed to start', 'red');
            $exit_code = 1;
        }
    }

    return $exit_code;
}

sub do_status {
    my ($self) = @_;

    my @expected_processes;
    push @expected_processes, "main-$_" foreach (1..$self->_num_of_main);
    push @expected_processes, "standby-$_" foreach (1..$self->_num_of_standby);

    foreach my $name (@expected_processes) {
        my $pidfile = $self->_build_pid_file($name);
        my $pid = $self->_read_pid_file($pidfile);

        if ($pid && $self->pid_running($pid)) {
            $self->pretty_print($name, 'Running');
        } else {
            $self->pretty_print($name, 'Not Running', 'red');
        }
    }
}

sub do_stop {
    my ($self) = @_;
    my $exit_code = 0;

    # start killing processes from standby
    my @expected_processes;
    push @expected_processes, "standby-$_" foreach (1..$self->_num_of_standby);
    push @expected_processes, "main-$_" foreach (1..$self->_num_of_main);

    NAME:
    foreach my $name (@expected_processes) {
        my $pidfile = $self->_build_pid_file($name);
        my $pid = $self->_read_pid_file($pidfile);

        if ($pid && $self->pid_running($pid)) {
            foreach my $signal (qw(TERM TERM INT KILL)) {
                $self->trace("Sending $signal signal to pid $pid...");
                kill($signal, $pid);

                my $tries = $self->kill_timeout // 0;
                while ($tries-- && $self->pid_running($pid)) {
                    sleep 1;
                }

                last unless $self->pid_running($pid);
            }

            if ($self->pid_running($pid)) {
                $self->pretty_print($name, 'Failed to stop', 'red');
                $exit_code = 1;
                next NAME;
            } else {
                $self->pretty_print($name, 'Stopped');
            }

            my $npid = $self->_read_pid_file($pidfile);
            if ($npid && $npid == $pid) {
                $self->_unlink_pid_file($pidfile);
            }
        } else {
            $self->pretty_print($name, 'Not Running', 'red');
            $self->_unlink_pid_file($pidfile);
        }
    }

    return $exit_code;
}

sub do_restart {
    return 1;
}

sub do_hard_restart {
    my ($self) = @_;
    $self->do_stop();
    return $self->do_start();
}

sub do_spawn {
    return 0;
}

#####################################
# routines to detect running process
#####################################
sub pid_running {
    my ($self, $pid) = @_;
    my $res = $pid && $pid > 1 ? kill(0, $pid) : 0;
    $self->trace("pid $pid is " . ($res ? 'running' : 'not running'));
    return $res;
}

#####################################
# forking functions
#####################################
sub _fork {
    my ($self) = @_;
    my $feedback = '';
    $self->trace("_double_fork()");

    my ($pread, $pwrite);
    pipe($pread, $pwrite) or die "Cannot open a pipe: $!";

    my $pid = fork();
    $pid and $self->trace("forked $pid");

    if ($pid == 0) { # Child, launch the process here
        close($pread); # Close reading end of pipe
        POSIX::setsid(); # Become the process leader

        my $new_pid = fork();
        $new_pid and $self->trace("forked $new_pid");

        if ($new_pid == 0) { # Our double fork.
            # close all file handlers
            my $pwrite_fd = fileno($pwrite);
            my $max_fd = POSIX::sysconf( &POSIX::_SC_OPEN_MAX );
            $max_fd = 64 if !defined $max_fd or $max_fd < 0;
            POSIX::close($_) foreach grep { $_ != $pwrite_fd } (3 .. $max_fd);

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

            my $res = $self->_launch_program($pwrite);
            exit($res // 0);
        } elsif (not defined $new_pid) {
            warn "Cannot fork: $!";
            POSIX::_exit(1);
        } else {
            POSIX::_exit(0);
        }
    } elsif (not defined $pid) { # We couldn't fork =(
        die "Cannot fork: $!";
    } else { # In the parent, $pid = child's PID, return it
        close($pwrite); # Close writting end of pipe

        $self->trace("waitpid()");
        waitpid($pid, 0); # Wait until first kid terminates

        $self->trace("sysread()");
        sysread($pread, $feedback, 128); # Read answer, block here
        close($pread);
    }

    $self->trace("got feedback: $feedback");
    return $feedback;
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
    my ($self, $pipe) = @_;
    $self->trace("_launch_program()");

    my $pid_file = $self->_build_pid_file("unknown-$$");
    $self->_write_pid_file($pid_file, $$);
    $self->{pid_file} = $pid_file;

    my $limit = $self->{limit};
    my $retiries_classback = $limit->{retries};

    my $self_weak = $self;
    weaken($self_weak);

    $limit->{retries} = sub {
        if ($self_weak) {
            # TODO fix potential memory leak
            my $id = $self_weak->{limit}->{standby_lock}->lock_id();
            $self_weak->trace("acquired standby lock $id");

            # adjusting name of pidfile
            my $pid_file = $self_weak->_build_pid_file("standby-$id");
            $self_weak->_rename_pid_file($self_weak->{pid_file}, $pid_file);
            $self_weak->{pid_file} = $pid_file;

            # sending feedback that we're in standby mode
            if ($pipe) {
                $self_weak->_syswrite_with_timeout($pipe, "standby-$id");
                close($pipe);
                $pipe = undef;
            }

            $self_weak = undef;
        }

        return $retiries_classback;
    };

    my $id = $limit->get_lock();
    
    if (defined $pipe) {
        $self->_syswrite_with_timeout($pipe, $id ? "main-$id" : 'failed');
        close($pipe);
    }

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
    my ($self, $name) = @_;
    return catfile($self->pid_dir, "$name.pid");
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
# pipe routines
#####################################
sub _syswrite_with_timeout {
    my ($self, $pipe, $msg) = @_;
    $self->trace("_syswrite($msg)");
    syswrite($pipe, $msg, length($msg));
}

sub _sysread_with_timeout {
    my ($self, $pipe, $len) = @_;
    $self->trace("_sysread($len)");
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

1;
