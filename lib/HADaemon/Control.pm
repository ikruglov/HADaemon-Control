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
my @accessors = qw( pid pid_dir pid_file quiet color_map name );
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
        quiet         => 1,
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

sub do_start {
    my ($self) = @_;

    my $limit_options = $self->{limit_options};
    my $num_of_forks = ($limit_options->{max_procs} // 0)
                     + ($limit_options->{standby_max_procs} // 0);

    for (1..$num_of_forks) {
        my $feedback = $self->_fork() // '';
        my ($message, $color) = $feedback =~ /^(?:main|standby)$/
                              ? ("Started - $feedback", 'green')
                              : ("Started - $feedback", 'green');
                              #: ('Failed', 'red');

        $self->pretty_print($message, $color);
    }
}

sub do_status {
    my ($self) = @_;

    my $limit_options = $self->{limit_options};
    my $num_of_main = $limit_options->{max_procs} // 0;
    my $num_of_standby = $limit_options->{standby_max_procs} // 0;

    my @expected_processes;
    push @expected_processes, "main-$_" foreach (1..$num_of_main);
    push @expected_processes, "standby-$_" foreach (1..$num_of_standby);

    foreach my $name (@expected_processes) {
        my $pidfile = catfile($self->pid_dir, "$name.pid");
        my $pid = $self->_read_pid_file($pidfile);
        my $is_running = $self->pid_running($pid);

        my ($msg, $color) = $is_running
                            ? ("Running - $name", 'green')
                            : ("Not running - $name", 'red');

        $self->pretty_print($msg, $color);
    }
}

sub pretty_print {
    my ($self, $message, $color) = @_;
    #return if $self->quiet;

    $color //= "green"; # Green is no color.
    my $code = $self->color_map->{$color} //= 32; # Green is invalid.

    local $| = 1;
    printf("%-49s %30s\n", $self->name, "\033[$code" ."m[$message]\033[0m");
}

sub trace {
    my ($self, $message) = @_;
    return if $self->quiet;
    print "$$ $message\n";
}

sub pid_running {
    my ($self, $pid) = @_;
    return 0 unless $pid;
    return 0 if $pid < 1;
    return kill 0, $pid;
}

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

    $self->pid($$);
    $self->pid_file(catfile($self->pid_dir, "unknown-$$.pid"));
    $self->_write_pid_file();

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
            my $pid_file = catfile($self_weak->pid_dir, "standby-$id.pid");
            $self_weak->_rename_pid_file($pid_file);

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
        $self->_unlink_pid_file();
        $self->trace("running program FAILED");
        exit(1); # TODO call callback
    }

    $self->trace("acquired main lock id: " . $limit->lock_id());
    
    # now pid file should be 'main-$id'
    my $pid_file = catfile($self->pid_dir, "main-$id.pid");
    $self->_rename_pid_file($pid_file);

    $self->trace("running program OK");
    sleep(10);
    $self->trace('exiting');
}

sub _read_pid_file {
    my ($self, $pid_file) = @_;
    return unless -f $pid_file;

    open(my $fh, "<", $pid_file) or die "Failed to read $pid_file: $!";
    my $pid = do { local $/; <$fh> };
    close $fh;

    return $pid;
}

sub _write_pid_file {
    my ($self) = @_;

    open(my $sf, ">", $self->pid_file) or die "Failed to write " . $self->pid_file . ": $!";
    print $sf $self->pid;
    close($sf);

    $self->trace("wrote pid (" . $self->pid . ") to pid file (" . $self->pid_file . ")");
    return $self;
}

sub _rename_pid_file {
    my ($self, $new_pid_file) = @_;
    return unless $self->pid_file;
    
    my $old_pid_file = $self->pid_file;
    rename($old_pid_file, $new_pid_file) or die "Failed to rename pidfile: $!";
    $self->pid_file($new_pid_file);

    $self->trace("rename pid file ($old_pid_file) to ($new_pid_file)");
    return $self;
}

sub _unlink_pid_file {
    my ($self) = @_;
    return unless $self->pid_file;

    unlink($self->pid_file);
    $self->trace("unlink pid file (" . $self->pid_file . ")");
    return $self;
}

sub _syswrite_with_timeout {
    my ($self, $pipe, $msg) = @_;
    $self->trace("_syswrite($msg)");
    syswrite($pipe, $msg, length($msg));
}

sub _all_actions {
    my ($self) = @_; 
    no strict 'refs';
    return map { m/^do_(.+)/ ? $1 : () } keys %{ ref($self) . '::' };
}

1;
