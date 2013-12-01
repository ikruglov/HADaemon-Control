HADaemon-Control
================

root@debian:~/HADaemon-Control# ./test.pl start
test.pl main #1                                                        [Started]
test.pl standby #1                                                     [Started]
test.pl standby #2                                                     [Started]

root@debian:~/HADaemon-Control# ./test.pl status
test.pl main #1                                                        [Running]
test.pl standby #1                                                     [Running]
test.pl standby #2                                                     [Running]

root@debian:~/HADaemon-Control# ./test.pl stop
test.pl standby #1                                                     [Stopped]
test.pl standby #2                                                     [Stopped]
test.pl main #1                                                        [Stopped]

