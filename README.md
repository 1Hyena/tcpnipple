# TCP Nipple v1.15
This Linux program is a proxy client that joins together pairs of outgoing TCP
connections. It connects simultaneously to two hosts (_supply_ and _demand_) and
exchanges data between them.

```
Usage: ./tcpnipple [options] supply-host supply-port demand-host demand-port
Options:
  -c  --connections   Number of connections to make (1).
      --brief         Print brief information (default).
  -h  --help          Display this usage information.
      --verbose       Print verbose information.
  -v  --version       Show version information.
```
