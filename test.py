#!/usr/bin/python

from peejay import *

def main () :

    pm = master ()
    pj = pm.run_pilot ()

    print str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    
    sleep (2)

    j = pj.job_submit ('touch /tmp/pj.test')

    print str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    print str(os.getpid()) + '  j: ' + str( j.get_state ())

    sleep (2)
    
    print str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    print str(os.getpid()) + '  j: ' + str( j.get_state ())
    
    pm.kill_pilot (pj)
    
    sleep (2)
    
    print str(os.getpid()) + ' pj: ' + str(pj.get_state ())


if __name__ == '__main__':
    main()
#   main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

