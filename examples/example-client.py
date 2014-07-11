#!/usr/bin/python
"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import re
import sys
import time
import socket
import platform
import subprocess

CARBON_SERVER = '127.0.0.1'
CARBON_PORT = 2013
DELAY = 60
COUNT = 1

def get_loadavg():
    """
    Get the load average for a unix-like system.
    For more details, "man proc" and "man uptime"
    """
    if platform.system() == "Linux":
        return open('/proc/loadavg').read().split()[:3]
    else:
        command = "uptime"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        stdout = process.communicate()[0].strip()
        # Split on whitespace and commas
        output = re.split("[\s,]+", stdout)
        return output[-3:]

def run(sock, delay, count):
    """Make the client go go go"""
    while True:
        now = int(time.time())
        print "Now = %d" % now
        lines = []
        #We're gonna report all three loadavg values
        loadavg = get_loadavg()
        for cnt in range(0, count):
            lines.append("system.loadavg_%d_1min %s %d" % (cnt, loadavg[0], now))
            lines.append("system.loadavg_%d_5min %s %d" % (cnt, loadavg[1], now))
            lines.append("system.loadavg_%d_15min %s %d" % (cnt, loadavg[2], now))
        message = '\n'.join(lines) + '\n' #all lines must end in a newline
#        print "sending message"
#        print '-' * 80
#        print message
        sock.sendall(message)
        d = delay - (int(time.time()) - now)
        time.sleep(d if d > 0 else 0)

def main():
    """Wrap it all up together"""
    delay = DELAY
    count = COUNT
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)
    if len(sys.argv) > 2:
        arg = sys.argv[2]
        if arg.isdigit():
            count = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % count)

    sock = socket.socket()
    try:
        sock.connect( (CARBON_SERVER, CARBON_PORT) )
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT })

    try:
        run(sock, delay, count)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
