#!/usr/bin/python

""" peejay is a stupid, simple pilot job (PJ, doh!) implementation.  I 
    mean, it is *really* stupid and simple, and not of much use.  Apart 
    from playing with the PJ paradigm, that is...
"""

import os
import subprocess
from   time import sleep

# simple helper call which, in a given dir, creates a given file with the given
# content
def echo (d, f, c) :
    
    tgt = str(d) + '/' + str(f)
    cmd = 'echo ' + str(c) + ' > ' + tgt
    print ' --> ' + cmd

    run (cmd)
    return tgt


# simple helper call which is the inverse of the echo above
def cat (d, f) :
    tgt = str(d) + '/' + str(f)

    c   = run ('cat ' + tgt).rstrip ()

    print ' --> cat ' + tgt + ' -> ' + c

    return c


# simple helper call which creates a dir/subdir and does not care about errors
def mkdir (d, s="") :
    tgt = str(d) + '/' + str(s)
    print ' --> mkdir ' + tgt

    try    : subprocess.call (['sh', '-c', 'mkdir -p ' + tgt])
    except Exception as e: print 'mkdir failed: ' + str(e) 

    return tgt


# simple helper call which movees a file
def mv (d_1, f_1, d_2, f_2 = '') :
    src = str(d_1) + '/' + str(f_1)
    tgt = str(d_2) + '/' + str(f_2)
    print ' --> mv ' + src + ' ' + tgt

    try    : subprocess.call (['sh', '-c', 'mv ' + src + ' ' + tgt])
    except Exception as e: print 'mv failed: ' + str(e) 


# simple helper call which deletes a file
def rm (d, f) :
    tgt = str(d) + '/' + str(f)
    print ' --> rm ' + tgt

    try    : subprocess.call (['sh', '-c', 'rm -f ' + tgt])
    except Exception as e: print 'rm failed: ' + str(e) 


# simple helper call which runs a shell command
def run (cmd) :
    c = ''
    print ' --> run ' + cmd

    try    : c = subprocess.Popen(['sh', '-c', cmd], stdout=subprocess.PIPE).communicate()[0]
    except Exception as e: print 'run failed: ' + str(e) 

    return c


# simple helper call which changes file modes
def chmod (d, f, m) :
    tgt = str(d) + '/' + str(f)
    print ' --> chmod ' + m + ' ' + tgt

    try    : subprocess.call (['sh', '-c', 'chmod ' + m + ' ' + tgt])
    except Exception as e: print 'chmod failed: ' + str(e) 


# simple helper call which lists some files in some dir
def ls (d, f) :
    tgt = str(d) + '/' + str(f)
    print ' --> ls ' + tgt
    files = ()

    try    : files = subprocess.Popen(['sh', '-c', 'ls ' + tgt], stdout=subprocess.PIPE).communicate()[0]
    except Exception as e: print 'ls failed: ' + str(e) 

    return files.splitlines ()



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


class master:
    """ me master class.  Create an instance, and that is it - your PJ framework
        is ready.
    """

    # these are class attributes, but thus valid per application instance, not
    # per class instance (go figure...)
    base      = '/tmp/peejay/' # base spool dir
    pilot_num = 0              # consecutive id for pilots, with obvious
                               # overrun problem

    def __init__ (self) :
        pass

    def pilot_base (self, pilot_id) :
        return self.base + '/' + str(pilot_id)


    def run_pilot (self) :
        # now, this routine is not thread safe -- if called concurrently, the
        # pilots will certainly confuse their IDs, and will screw up the spool
        # dir management which relies on unique IDs.  So, don't.
        self.pilot_num += 1
        pilot_id = str(self.pilot_num)

        print 'peejay master spawns pilot ' + pilot_id

        
        # prepare pilot base
        pilot_base = self.pilot_base (pilot_id)

        mkdir (pilot_base)
        echo  (pilot_base, 'state', state.New)

        # create the pilot, and init it
        pilot_pid = os.fork ()

        if pilot_pid :
            # keep the pilot pid around
            echo (pilot_base, 'pid', pilot_pid)

            # tell callee about spawned pilot
            return pilot (pilot_id)

        else :
            # this is the pilot.  Initialize, serve, and die.  Tough life, ey?
            p = pilot (pilot_id)
            p.serve ( )

            # the fork finishes here...
            exit (0)

    def kill_pilot (self, pilot) :

        pilot.kill ()



class pilot:

    def __init__ (self, pilot_id) :

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

        self.pilot_id = str(pilot_id)
        self.base     = mkdir (master.base, pilot_id)
        self.spool    = mkdir (self.base, 'spool')
        self.active   = mkdir (self.base, 'active')
        self.done     = mkdir (self.base, 'done')
        self.jobnum   = 0


    def job_submit (self, command) :
        self.jobnum += 1
        job_id = self.jobnum

        name = 'job.' + str(job_id)
        echo (self.spool, name, command)

        return job (self, job_id)


    def job_get_state (self, job_id) :
        base = 'job.' + str(job_id)
        files = ls (self.base + '/*/', base + '*')

        for f in files :
            elems = f.split ('/')
            n = elems.pop ()
            d = elems.pop ()

            while d == "" :
                d = elems.pop ()

            if d == 'spool'  : return state.New
            if d == 'active' : return state.Running
            if d == 'done'   : 
                if n == base + '.ok'  : return state.Done
                if n == base + '.nok' : return state.Failed

        return state.Unknown


    def is_active (self) :
        if not self.get_state () == state.Running :
            raise Exception ('pilot is not in Running state')


    def kill (self) :
        if self.get_state () == state.Running :
            echo (self.spool, 'COMMAND', 'QUIT')


    def set_state (self, state) :
        echo (self.base, 'state', state)


    def get_state (self) :
        return state.str2state (cat (self.base, 'state'))


    def serve (self) :
        print 'serving: ' + self.pilot_id
        self.set_state (state.Running)

        while 1 :
            # get all items in spool dir
            list = os.listdir (self.spool)

            if not len (list) :
                print 'peejay pilot ' + self.pilot_id + ': nothing to do'
                sleep (1)

            else :
                for item in list :
                    if item == 'COMMAND' :

                        # yes master, got it
                        command = cat (self.spool, item)
                        rm (self.spool, item)

                        if command == 'QUIT' :
                           self.set_state ( state.Canceled)
                           exit (0)
                        else :
                            print 'do not know how to handle command ' + command
                            print 'ignored :-P'

                    else : # item != COMMAND

                      print 'serving ' + item + ' (' + str (os.getpid()) + ')'

                      # well, lts run that bugger in a separate process!
                      job_pid = os.fork ()

                      if job_pid :
                          # this main fork does nothing, and continues to loop through
                          # the list of items
                          
                          # 'does nothing' : the sleep is actually needed to let
                          # the forked process move the script away from spool
                          sleep (1)
                          pass

                      else :
                          # here we are in the forked process, now fully dedicated
                          # to pamper (i.e. to wrap) the job
                          pid = str(os.getpid ())

                          script      = item
                          script_run  = item + '.run'
                          script_out  = item + '.out'
                          script_err  = item + '.err'
                          script_ok   = item + '.ok' 
                          script_nok  = item + '.nok'
                          script_all  = item + '*'

                          print 'pilot ' + self.pilot_id + ' : running : ' + item

                          command =                self.active + '/' + script_run \
                                  + ' 1> '       + self.done   + '/' + script_out \
                                  + ' 2> '       + self.done   + '/' + script_err \
                                  + ' && touch ' + self.done   + '/' + script_ok  \
                                  + ' || touch ' + self.done   + '/' + script_nok 

                          mv    (self.spool, script,      self.active, script_run)
                          chmod (self.active, script_run, '0755')
                          run   (command)
                          mv    (self.active, script_all, self.done)

                          print 'pilot ' + self.pilot_id + ': done    : ' + item

                          # job is done, files are staged out, bye bye
                          exit (0)



class job:

    def __init__ (self, pilot, job_id) :

        self.pilot    = pilot
        self.job_id   = job_id
        self.canceled = False


    def kill (self) :
        pilot.job_kill (self.job_id)
        self.canceled = True


    def get_state (self) :
        if self.canceled : return state.Canceled
        return self.pilot.job_get_state (self.job_id)
        


def main () :

    pm = master ()
    pj = pm.run_pilot ()

    print 'pj: ' + str(pj.get_state ())
    
    sleep (2)

    j = pj.job_submit ('touch /tmp/pj.test')

    print 'pj: ' + str(pj.get_state ())
    print ' j: ' + str( j.get_state ())

    sleep (2)
    
    print 'pj: ' + str(pj.get_state ())
    print ' j: ' + str( j.get_state ())
    
    pm.kill_pilot (pj)
    
    sleep (2)
    
    print 'pj: ' + str(pj.get_state ())


if __name__ == '__main__':
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

