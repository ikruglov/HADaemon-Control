package HADaemon::Control;

use strict;
use warnings;

use File::Spec;
use File::Spec::Functions;
use File::Basename;
use Scalar::Util qw(weaken);
use IPC::ConcurrencyLimit::WithStandby;
use Data::Dumper;
use POSIX qw(_exit setsid);

# Accessor building
my @accessors = qw( pid_dir quiet color_map name );
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
    #$self->program or die "Error: program must be defined";
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
        and exit($self->$action() // 1);

    die "Error: unknown action $called_with. [$allowed_actions]";
}

#####################################
# commands
#####################################
sub do_start {
    my ($self) = @_;
    my $num_of_forks = $self->_num_of_main() + $self->_num_of_standby();

    for (1..$num_of_forks) {
        my $feedback = $self->_fork() // '';
        if ($feedback =~ m/^(?:main|standby)-\d+$/) {
            $self->pretty_print($feedback, 'Started');
        } else {
            $self->pretty_print('', 'Failed to start', 'red');
        }
    }
}

sub do_status {
    my ($self) = @_;

    my @expected_processes;
    push @expected_processes, "main-$_" foreach (1..$self->_num_of_main);
    push @expected_processes, "standby-$_" foreach (1..$self->_num_of_standby);

    foreach my $name (@expected_processes) {
        my $pidfile = $self->_build_pid_file($name);
        my $pid = $self->_read_pid_file($pidfile);
        my $is_running = $self->pid_running($pid);

        if ($pid && $is_running) {
            $self->pretty_print($name, 'Running');
        } else {
            $self->pretty_print($name, 'Not Running', 'red');
        }
    }
}

#####################################
# routines to detect running process
#####################################
sub pid_running {
    my ($self, $pid) = @_;
    return 0 unless $pid;
    return 0 if $pid < 1;
    return kill 0, $pid;
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
        setsid(); # Become the process leader

        my $new_pid = fork();
        $new_pid and $self->trace("forked $new_pid");

        if ($new_pid == 0) { # Our double fork.
            $self->_launch_program($pwrite);
        } elsif (not defined $new_pid) {
            warn "Cannot fork: $!";
            _exit 1;
        }

        _exit 0;
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
        exit(1); # TODO call callback
    }

    $self->trace("acquired main lock id: " . $limit->lock_id());
    
    # now pid file should be 'main-$id'
    $pid_file = $self->_build_pid_file("main-$id");
    $self->_rename_pid_file($self->{pid_file}, $pid_file);
    $self->{pid_file} = $pid_file;

    sleep(10);
    $self->trace('exiting');
}

#####################################
# pid file routines
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

    $self->trace("read pid " . ($pid // 'undef') . " from pid file ($pid_file)");
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
    unlink($pid_file);
    $self->trace("unlink pid file ($pid_file)");
    return $self;
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
    return if $self->quiet;
    #print "$$ $message\n";
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
