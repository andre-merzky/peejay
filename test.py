#!/usr/bin/python

from peejay import *

def main () :

    pm = master ()
    pj = pm.run_pilot ()

    print "PEEJAY " + str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    
    # j = pj.job_submit ('touch /tmp/pj.test ; sleep 2')

    j = pj.job_submit ('/bin/sh -c "sleep 10"')
    # j = pj.job_submit ('/bin/sh -c "touch /tmp/hello_troy_pj && sleep 10"')


    print "PEEJAY " + str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    print "PEEJAY " + str(os.getpid()) + '  j: ' + str( j.get_state ())

    j.wait ()

    # j_copy = 

    print "PEEJAY " + str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    print "PEEJAY " + str(os.getpid()) + '  j: ' + str( j.get_state ())
    
    pm.kill_pilot (pj)
    
    print "PEEJAY " + str(os.getpid()) + ' pj: ' + str(pj.get_state ())
    print "PEEJAY " + str(os.getpid()) + '  j: ' + str( j.get_state ())


if __name__ == '__main__':
    main()
#   main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

