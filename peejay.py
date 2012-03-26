#!/usr/bin/python

""" peejay is a stupid, simple pilot job (PJ, doh!) implementation.  I 
    mean, it is *really* stupid and simple, and not of much use.  Apart 
    from playing with the PJ paradigm, that is...
"""

import os
import re
import subprocess
from   time import sleep

class peejay:
    """ me master class.  create an instance, and that is it - your PJ framework
        is ready.

        If created in application, it will run master(), and spawn one pilot
        job.  That pilot job is a python agent, run like that

          /bin/env PEEJAY_ID=1 PEEJAY_BASE=/tmp/peejay/ python -c "import peejay; pj=new peejay()"

        which will also just instantiate a peejay instance - that will run
        serve(), and will pickup work in /tmp/peejay/1.
    """

    def __init__ (self) :
        self.id     = "0"            # master = 0, pilots > 0
        self.pilots = []             # list of pilot pid's
        self.jobs   = []             # list of jobs within the pilots
        self.base   = "/tmp/peejay/" # base spool dir

        self.init ()


    def init (self, pilot_id = "0") :

        self.id = str (pilot_id)
        print "peejay: id  " + self.id


        if self.id == "0" :
            self.master = True
        else :
            self.master = False


        if self.master :
            print "peejay master spawns pilot"
            self.pilots.append (self.create_pilot ())

        else :
            self.base = "/tmp/peejay/" + self.id

            try    : os.mkdir (self.base)
            except : pass

            print " peejay: base: " + self.base




    def create_pilot (self) :

        # only master can create pilots
        if not self.master :
            print "peejay agent running wild!  shut Down  EVERYTHING!!!"
            exit (-1)

        # fork returns with 0 for the spawned process
        pilot_pid = os.fork ()

        if not pilot_pid :
            # this is the pilot.  Initialize, serve, and die.  Tough life, ey?
            pilot_id = len (self.pilots) + 1
            self.init  (pilot_id)
            self.serve ( )
            exit  (0)

        # tell master about spawned pilot
        return pilot_pid





    def serve (self) :

        print "serving: " + self.id

        while 1 :
            # get all items in dir
            list = os.listdir (self.base)


            # any order assumed
            if not len (list) :

                print "peejay pilot " + self.id + ": nothing to do"

            else :

                for item in list :

                    name_parts = re.split ("\.", item)

                    # *,run: job to run
                    # *.cmd: command to interpret
                    action = name_parts [-1]
                    print "action: " + action

                    if action == "quit" :
                        print "peejay pilot was told to quit. Well then... - bye"
                        os.remove (self.base + "/" + item)
                        return

                    if action == "run" :

                        job_pid = os.fork ()

                        if not job_pid :
                            script        = self.base     + "/" + item
                            script_active = script        + '.' + str (os.getpid ())
                            script_done   = script_active + '.done'

                            print "pilot " + self.id + ": running : " + script

                            os.rename (script,                 script_active)
                            subprocess.call (['/bin/sh', '-e', script_active])
                            os.rename (script_active,          script_done)

                            print "pilot " + self.id + ": done    : " + script
                            exit (0)

            sleep (1)





pj = peejay ()





# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

