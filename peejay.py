#!/usr/bin/python

""" peejay is a stupid, simple pilot job (PJ, doh!) implementation.  I 
    mean, it is *really* stupid and simple, and not of much use.  Apart 
    from playing with the PJ paradigm, that is...
"""

import re
import os
import sys
import subprocess
import random
import traceback
from   time import sleep


################################################################################
# simple helper call which runs a shell command
def run (cmd) :
    r    = 0
    out  = ''
    err  = ''
    fail = False
    pid  = os.getpid()

    try    : 
        p    = subprocess.Popen(['sh', '-c', cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        io   = p.communicate()
        out  = io[0]
        err  = io[1]
        r    = p.returncode
        if r :
            fail = True

    except Exception as e: 
        fail = True


    if fail :
        print "peejay " + str(os.getpid()) + ' ----------------- run failed'
        print "peejay " + str(os.getpid()) + " cmd : " + str(cmd)
        print "peejay " + str(os.getpid()) + " out : " + str(out)
        print "peejay " + str(os.getpid()) + " err : " + str(err)
        print "peejay " + str(os.getpid()) + " r   : " + str(r)
        print "peejay " + str(os.getpid()) + ' ----------------- '
        # exit (-1)

    return out



# " -> \"
def quote (s) :
    
    ret = ""
    for c in str(s) :
        if c == '"' :
            ret += '\\'
        ret += c
    return ret


# simple helper call which, in a given dir, creates a given file with the given
# content
def echo (d, f, c) :
    
    tgt = str(d) + '/' + str(f)
    cmd = 'echo "' + quote(c) + '" > ' + tgt

    run (cmd)
    return tgt


# simple helper call which is the inverse of the echo above
def cat (d, f) :
    tgt = str(d) + '/' + str(f)
    c   = run ('cat ' + tgt).rstrip ()

    return c


# simple helper call which creates a dir/subdir and does not care about errors
def mkdir (d, s="") :
    tgt = str(d) + '/' + str(s)

    run ('mkdir -p ' + tgt)

    return tgt


# simple helper call which copies a file
def cp (d_1, f_1, d_2, f_2 = '') :
    src = str(d_1) + '/' + str(f_1)
    tgt = str(d_2) + '/' + str(f_2)

    run ('cp ' + src + ' ' + tgt)


# simple helper call which moves a file
def mv (d_1, f_1, d_2, f_2 = '') :
    src = str(d_1) + '/' + str(f_1)
    tgt = str(d_2) + '/' + str(f_2)

    run ('mv ' + src + ' ' + tgt)


# simple helper call which deletes a file
def rm (d, f) :
    tgt = str(d) + '/' + str(f)

    run ('rm -f ' + tgt)


# simple helper call which changes file modes
def chmod (d, f, m) :
    tgt = str(d) + '/' + str(f)

    run ('chmod ' + m + ' ' + tgt)


# simple helper call which lists some files in some dir
def ls (d, f="") :
    tgt = str(d) + '/' + str(f)

    out   =  run ('ls ' + tgt)

    lines = out.splitlines ()

    # remove trailing '/'
    for line in lines :
        line.rstrip ('/')

    return lines


###################################################################
#
# create and parse IDs
#
def create_master_id (master_idx) :
    return '[' +  str(master_idx) + ']'


def create_pilot_id (master_idx, pilot_idx) :
    return '[' + str(master_idx) + ']-[' + str(pilot_idx) + ']'


def create_job_id   (master_idx, pilot_idx, job_idx) :
    return '[' + str(master_idx) + ']-[' + str(pilot_idx) + ']-[' + str(job_idx) + ']'



def parse_master_id (master_id) :
    match = re.match (r'^\[(\d+)\]$', master_id)
    if match is None : return None
    return match.groups()[0]


def parse_pilot_id (pilot_id) :
    match = re.match (r'^\[(\d+)\]-\[(\d+)\]$', pilot_id)
    if match is None : return [None, None]
    return [match.groups()[0], match.groups()[1]]


def parse_job_id (job_id) :
    match = re.match (r'^\[(\d+)\]-\[(\d+)\]-\[(\d+)\]$', job_id)
    if match is None : return [None, None, None]
    return [match.groups()[0], match.groups()[1], match.groups()[2]]



################################################################################
class state ():
    Unknown  = 'Unknown'
    New      = 'New'
    Pending  = 'Pending'
    Running  = 'Running'
    Done     = 'Done'
    Canceled = 'Canceled'
    Failed   = 'Failed'

    @staticmethod
    def str2state (s) :

        if s == 'New'      : return state.New     
        if s == 'Pending'  : return state.Pending 
        if s == 'Running'  : return state.Running 
        if s == 'Done'     : return state.Done    
        if s == 'Canceled' : return state.Canceled
        if s == 'Failed'   : return state.Failed  
        return state.Unknown


################################################################################
class master:
    """ me master class.  Create an instance, and that is it - your PJ framework
        is ready.
    """

    # these are class attributes, but thus valid per application instance, not
    # per class instance (go figure...)
    root       = '/tmp/peejay' # all master pilots live beneath
    master_idx = 1             # consecutive id for masters, with obvious
                               # overrun problem.  FIXME: needs to be larger than any
                               # re-connectible master

    def __init__ (self, master_id = None) :

        if master_id == None :
            self.reconnect     = False
            self.master_idx    = master.master_idx
            self.master_id     = create_master_id (self.master_idx)
            master.master_idx += 1
        else :
            self.reconnect  = True
            self.master_idx = parse_master_id (master_id)
            self.master_id  = str(master_id)


        self.base        = master.root + '/' +  str(self.master_idx)
        self.pilot_idx   = 0
        self.pilots      = []

        # gather infos about existing pilots
        if self.reconnect :
            pilot_nums = ls (self.base, '')
            for n in pilot_nums :
                self.pilots.append (create_pilot_id (self.master_idx, n))


    def get_base (self) :
        return self.base

    def get_id (self) :
        return self.master_id

    def pilot_base (self, pilot_idx) :
        return self.base + '/' + str(pilot_idx)


    def run_pilot (self) :
        # now, this routine is not thread safe -- if called concurrently, the
        # pilots will certainly confuse their IDs, and will screw up the spool
        # dir management which relies on unique IDs.  So, don't.
        self.pilot_idx += 1
        pilot_id = create_pilot_id (self.master_idx, self.pilot_idx)

        
        # prepare pilot base
        pilot_base = self.pilot_base (self.pilot_idx)

        mkdir (pilot_base)
        echo  (pilot_base, 'state', state.New)

        # create the pilot, and init it
        sys.stdout.flush()
        sys.stderr.flush()
        pilot_pid = os.fork ()

        if pilot_pid :
            print "peejay " + str(os.getpid()) + " started p/home/merzky/pictures/import/2012-08-27/DCIMilot " + str(pilot_pid)

            # keep the pilot id and pid around
            echo (pilot_base, 'pid', pilot_pid)
            self.pilots.append (pilot_id)

            # init pilot, and tell callee about spawned pilot
            return pilot (pilot_id, init=True)

        else :

            self.pilot_idx += 1
            # this is the pilot.  It got initialized - now serve and die.  Tough life, ey?
            print "peejay " + str(os.getpid()) + " pilot started -- " + str(pilot_id) + " - " + str(self.master_idx)
            p = pilot (pilot_id)
            p.serve ( )

            # the fork finishes here...
            exit (0)



    def list_pilots (self) :
        return self.pilots


    def list_jobs (self) :

        ret = []
        for p_id in self.list_pilots () :
            p = self.get_pilot (p_id)
            ret += p.list_jobs ()
        return ret


    def get_pilot (self, pilot_id) :
        return pilot (pilot_id)



    def kill_pilot (self, pilot) :

        if not self.pilots.count (pilot.get_id ()) :
            raise Exception ("No such pilot registered")

        print "peejay " + str(os.getpid()) + " killing pilot " + str(pilot.get_id ())
        self.pilots.remove (pilot.get_id ())
        pilot.kill ()



################################################################################
class pilot:

    def __init__ (self, pilot_id, init=False) :

        # we use the following dir structure to manage jobs and state:
        #
        # /tmp/peejay/<pilot_id>/spool    - jobs are dropped here as shell scripts
        # /tmp/peejay/<pilot_id>/active   - jobs are moved here once running
        # /tmp/peejay/<pilot_id>/finished - jobs are moved here once final
        #
        # So, a job lifecycle is rendered as:
        #
        #   # cat >  /tmp/peejay/1/spool/job_1.sh
        #     /bin/date
        #
        #   That job is wrapped into a shell script, which is:
        #
        #     mv /tmp/peejay/1/spool/job_1.run /tmp/peejay/1/active/job_1.run
        #     sh -c      /tmp/peejay/1/active/job_1.sh.run \
        #       1 >      /tmp/peejay/1/active/job_1.sh.out \
        #       2 >      /tmp/peejay/1/active/job_1.sh.out \
        #       && touch /tmp/peejay/1/active/job_1.sh.ok  \
        #       || touch /tmp/peejay/1/active/job_1.sh.nok
        #     mv /tmp/peejay/1/active/job_1.* /tmp/peejay/1/finished/
        #
        #   We need to make sure of course that the jobs are named uniquely in
        #   the initial spool - and, what else, use a serial integer ID for
        #   that...

        self.pilot_id = pilot_id

        print "peejay pilot: " + str(pilot_id)

        [self.master_idx, self.pilot_idx] = parse_pilot_id (self.pilot_id)

        self.master_base  = master.root      + '/' + self.master_idx
        self.base         = self.master_base + '/' + self.pilot_idx
        self.active       = self.base        + '/' + 'active'
        self.done         = self.base        + '/' + 'done'
        self.spool        = self.base        + '/' + 'spool'
        self.job_idx      = self.base        + '/' + 'job_idx'

        self.description  = { 'type' : 'Compute' }
        self.jobs = []

        if init :
            # we need to initialize working space once
            if not os.path.exists (self.job_idx) :

                # initialize pilot state and job id index
                mkdir (self.base, 'active')
                mkdir (self.base, 'done')
                mkdir (self.base, 'spool')

                echo  (self.base, 'job_idx', 0)

    
    def get_job_idx (self) :
        job_idx  = int (cat (self.base, 'job_idx'))
        job_idx += 1
        echo (self.base, 'job_idx', job_idx)
        return job_idx

    def get_id (self) :
        return self.pilot_id


    def get_description (self) :
        return self.description


    def get_master_id (self) :
        return create_master_id (self.master_idx)

    def get_base (self) :
        return self.base

    def job_submit (self, command) :
        # generate a new job id
        job_idx = self.get_job_idx ()
        job_id  = create_job_id (self.master_idx, self.pilot_idx, job_idx)

        echo (self.spool, job_idx, command)

        ret = job (job_id)

        self.jobs.append (job_id)

        return ret


    def list_jobs (self) :
        return self.jobs


    def job_get_state (self, job_id) :

        [master_idx, pilot_idx, job_idx] = parse_job_id (job_id)

        if master_idx != self.master_idx : return state.Unknown 
        if pilot_idx  != self.pilot_idx  : return state.Unknown

        if os.path.exists (self.spool  + '/' + job_idx          ) : return state.Pending
        if os.path.exists (self.active + '/' + job_idx          ) : return state.Running
        if os.path.exists (self.done   + '/' + job_idx + '.ok'  ) : return state.Done
        if os.path.exists (self.done   + '/' + job_idx + '.nok' ) : return state.Failed

        return state.Unknown


    def is_active (self) :
        if not self.get_state () == state.Running :
            raise Exception ('pilot is not in Running state')

    def wait (self) :
        s = self.get_state () 
        while s != state.Done     and \
              s != state.Failed   and \
              s != state.Canceled : 

            print "peejay " + str(os.getpid()) + " wait for pilot " + str(self.get_id ()) + " : " +  str(s)
            sleep (1)
            s = self.get_state () 


    def kill (self) :
        if self.get_state () == state.Running :
            echo (self.spool, 'COMMAND', 'QUIT')
        # wait for final state
        # FIXME: avoid deadlock on error
        self.wait ()


    def set_state (self, state) :
        echo (self.base, 'state', state)


    def get_state (self) :
        return state.str2state (cat (self.base, 'state'))


    def serve (self) :
        print "peejay " + str(os.getpid()) + ' serving: ' + self.pilot_id
        self.set_state (state.Pending)

        # we wait until the spool directory appears - from that point on, we
        # consider ourself Running
        while not os.path.isdir (self.spool) :
            sleep (1)

        # yay, the fun begins!
        self.set_state (state.Running)
        

        while 1 :
            # get all items in spool dir
            # print "peejay " + str(os.getpid()) + ' peejay pilot ' + self.pilot_id + ': watching ' + self.spool
            items = ls (self.spool)

            if not items or not len (items) :
                # print "peejay " + str(os.getpid()) + ' peejay pilot ' + self.pilot_id + ': nothing to do'
                sleep (1)

            else :

                for item in items :
                
                    # print "peejay " + str(os.getpid()) + ' peejay pilot handles ' + item 

                    if item == 'COMMAND' :

                        # yes master, got it
                        command = cat (self.spool, item)
                        rm (self.spool, item)

                        if command == 'QUIT' :
                           self.set_state ( state.Canceled)
                           exit (0)
                        else :
                            print "peejay " + str(os.getpid()) + ' do not know how to handle command ' + command
                            print "peejay " + str(os.getpid()) + ' ignored :-P'

                    else : # item != COMMAND


                      # move script into active area
                      script_cmd  = item
                      script_run  = item + '.run'
                      script_out  = item + '.out'
                      script_err  = item + '.err'
                      script_ok   = item + '.ok' 
                      script_nok  = item + '.nok'
                      script_all  = item + '.*'

                      mv (self.spool,  script_cmd, self.active)
                      cp (self.active, script_cmd, self.active, script_run)
                      chmod (self.active, script_run, '0755')

                      # well, lts run that bugger in a separate process!
                      job_pid = os.fork ()

                      if job_pid :
                          # this main fork does nothing, and continues to loop through
                          # the list of items
                          
                          # 'does nothing' : the sleep is actually needed to let
                          # the forked process move the script away from spool
                          print "peejay " + str(os.getpid()) + " started job " + item + " : " + str(job_pid)

                      else :
                          # here we are in the forked process, now fully dedicated
                          # to pamper (i.e. to wrap) the job
                          print "peejay " + str(os.getpid()) + ' pilot ' + self.pilot_id + ' : running : ' + item
                          print "peejay " + str(os.getpid()) + " job starting "

                          command =                self.active + '/' + script_run \
                                  + ' 1> '       + self.done   + '/' + script_out \
                                  + ' 2> '       + self.done   + '/' + script_err \
                                  + ' && touch ' + self.done   + '/' + script_ok  \
                                  + ' || touch ' + self.done   + '/' + script_nok 


                          run (command)
                          mv (self.active, item      , self.done)
                          mv (self.active, script_all, self.done)

                          print "peejay " + str(os.getpid()) + ' pilot ' + self.pilot_id + ': done    : ' + item 

                          # job is done, files are staged out, bye bye
                          exit (0)



################################################################################
class job :

    def __init__ (self, job_id) :

        self.job_id = job_id
        [self.master_idx, self.pilot_idx, self.job_idx] = parse_job_id (self.job_id)

        self.canceled = False
        self.pilot = pilot (create_pilot_id (self.master_idx, self.pilot_idx))

        cmd = cat (self.pilot.get_base () + "/spool", self.job_idx)

        # create description from command
        words = cmd.split ()

        if len(words) >= 1 :
            exe = words[0]
        if len(words) >= 2 :
            args = words[1:]


        self.description = {'executable' : exe,
                            'arguments'  : args }


    def kill (self) :
        pilot.job_kill (self.job_id)
        self.canceled = True


    def get_state (self) :
        print "peejay " + str(os.getpid()) + " getting job state"
        if self.canceled : return state.Canceled
        return self.pilot.job_get_state (self.job_id)
        

    def get_id (self) :
        return self.job_id


    def get_pilot_id (self) :
        return self.pilot.get_id ()


    def get_master_id (self) :
        p = pilot (self.get_pilot_id ())
        return p.get_master_id ()


    def get_description (self) :
        return self.description


    def wait (self) :

        s = self.get_state () 
        while s != state.Done     and \
              s != state.Failed   and \
              s != state.Canceled : 

            print "peejay " + str(os.getpid()) + " wait for job " + str(self.job_id) + " : " +  str(s)
            sleep (1)
            s = self.get_state () 



# def main () :
# 
#     pm = master ()
#     pj = pm.run_pilot ()
# 
#     print ' pj: ' + str(pj.get_state ())
#     
#     sleep (2)
# 
#     j = pj.job_submit ('touch /tmp/pj.test')
# 
#     print ' pj: ' + str(pj.get_state ())
#     print '  j: ' + str( j.get_state ())
# 
#     sleep (2)
#     
#     print ' pj: ' + str(pj.get_state ())
#     print '  j: ' + str( j.get_state ())
#     
#     pm.kill_pilot (pj)
#     
#     sleep (2)
#     
#     print ' pj: ' + str(pj.get_state ())


# if __name__ == '__main__':
#     main()
#     main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

