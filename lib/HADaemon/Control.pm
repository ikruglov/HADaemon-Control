package HADaemon::Control;

use strict;
use warnings;

use File::Spec;
use Data::Dumper;
use IPC::ConcurrencyLimit::WithStandby;
use POSIX qw(_exit setsid);

# Accessor building
my @accessors = qw( pid_dir pid_file quiet color_map name );
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

sub do_start {
    my ($self) = @_;

    my $limit_options = $self->{limit_options};
    my $num_of_forks = ($limit_options->{max_procs} // 0)
                     + ($limit_options->{standby_max_procs} // 0);

    for (1..$num_of_forks) {
        my $feedback = $self->_fork() // '';
        my ($message, $color) = $feedback =~ /^(?:main|standby)$/
                              ? ("Started - $feedback", 'green')
                              : ('Failed', 'red');

        $self->pretty_print($message, $color);
    }
}

sub do_stop {}
sub do_status {}

sub _fork {
    my ($self) = @_;
    my $feedback = '';
    $self->trace("_double_fork()");

    my ($pread, $pwrite); # TODO check pipe status
    pipe($pread, $pwrite) or warn "Cannot open a pipe: $!";

    my $pid = fork();
    $self->trace("forked $pid") if $pid;
    if ($pid == 0) { # Child, launch the process here.
        close($pread) if $pread; # Close reading end of pipe
        setsid(); # Become the process leader.

        my $new_pid = fork();
        $self->trace("forked $new_pid") if $new_pid;
        if ($new_pid == 0) { # Our double fork.
            $self->_launch_program($pwrite);
            _exit 0;
        } elsif (not defined $new_pid) {
            warn "Cannot fork: $!";
            _exit 1;
        } else {
            #$self->pid($new_pid);
            $self->trace("Set PID => $new_pid");
            #$self->write_pid;
            _exit 0;
        }
    } elsif (not defined $pid) { # We couldn't fork.  =(
        close($pread) if $pread;
        close($pwrite) if $pwrite;
        warn "Cannot fork: $!";
    } else { # In the parent, $pid = child's PID, return it.
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

    my $limit = $self->{limit};
    my $retries_callback = $limit->{retries};
    $limit->{retries} = sub {
        $self->trace("retries()");
        if (defined $pipe) {
            my $msg = 'standby';
            $self->trace("syswrite(standby)");
            syswrite($pipe, $msg, length($msg));
            close($pipe);
            undef($pipe);
        }

        return $retries_callback;
    };

    my $id = $self->{limit}->get_lock();
    if (defined $pipe) {
        my $msg = $id ? 'main' : 'failed';
        $self->trace("syswrite($msg)");
        syswrite($pipe, $msg, length($msg));
        close($pipe);
        undef($pipe);
    }

    $self->trace("running program ". ($id ? "OK" : "FAILED"));
    $id or exit(1); # TODO call callback
    #$self->program->($self, @args);
    sleep(5);
    $self->trace('exiting');
}

sub pretty_print {
    my ($self, $message, $color) = @_;
    return if $self->quiet;

    $color //= "green"; # Green is no color.
    my $code = $self->color_map->{$color} //= 32; # Green is invalid.

    local $| = 1;
    printf("%-49s %30s\n", $self->name, "\033[$code" ."m[$message]\033[0m");
}

sub _all_actions {
    my ($self) = @_; 
    no strict 'refs';
    return map { m/^do_(.+)/ ? $1 : () } keys %{ ref($self) . '::' };
}

sub trace {
    my ($self, $message) = @_;
    print "$$ $message\n";
}

1;
