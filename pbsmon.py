
##Author: Majid al-Dosari
#Copyright (c) 2010, Majid al-Dosari
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the <organization> nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
python thisfile.py --help
"""

import os
import sys
pathname = os.path.dirname(sys.argv[0])
pathname= os.path.join(pathname,'pbsmgr.py') #look in same dir
__name__='notmain' #smartass :)
execfile(pathname)
del pathname
__name__ = '__main__'


def pbsjobmonitor(pbsmgrobj
    ,attribstomon=['job_state','pbsid'],refreshtime=60*5,by='pbsname'): #or by id       
    pmo=pbsmgrobj
    def gettable():
        pmo.genjobsinfotbl()
        if by=='pbsname': return pmo.jobinfobyname
        else: return pmo.jobinfobypbsid
    
    oldtbl=gettable()
    
    if len(oldtbl.keys())>=1:
        print 'current jobs:'
        for k,v in oldtbl.iteritems():
            print k, v
    else: print 'no current jobs'
    
    print 'ctl-c to break'
    print 'job changes:'
    while True:
        time.sleep(refreshtime)
        newtbl=gettable()
        changes=outputjobchanges(oldtbl,newtbl,attribstomon=attribstomon)
        if len(changes[0])>0:
            for ajobname in changes[0]:
                print 'added job: ', ajobname #could be by pbsid
        if len(changes[1])>0:
            for ajn in changes[1].keys():
                for anattrib in changes[1][ajn].keys():
                    print ajn ,' ', anattrib, changes[1][ajn][anattrib][1]
        if len(changes[2])>0:
            if by=='pbsname':
                for ajobname in changes[2]:
                    print 'removed job script: ', ajobname
        oldtbl=gettable()
    return

if __name__ == '__main__':
	from optparse import OptionParser
	usage='Monitors jobs in current and below directories. (no actions taken)'
	parser = OptionParser(usage=usage)
	parser.add_option("--by", dest="by",default='pbsname'
                  ,help="monitor jobs by pbsid or PBS jobname"
                  #,store='store'
                  ,type='choice',choices=['pbsid','pbsname']
                  )
	import getpass
	uid=getpass.getuser()
	parser.add_option("--userid",'-u', dest="userid",default=uid
              ,help="filter jobs in system by userid. default: "+uid)
	cd=os.getcwd()
	parser.add_option("--dir",'-d', dest="dirs",default=[cd]
                ,action='append'
              ,help="monitor jobs in dirs. you can specify more than one\
              dir by repeating the option. default is just cd: "+cd)
	parser.add_option("--refresh",'-r', dest="refresh",default=300
              ,help="refresh time. default: 300 secs",type=int)
	parser.add_option("--watch",'-w', dest="watch"
			,default=['job_state','pbsid']
			,help="monitor pbs attribs. multiple attribs specified by multiple options. \
			default: job_state and pbsid",type='choice'
			,choices=pbsattribstouse,action='append')
	(options, args) = parser.parse_args()
        
	mainpbsmgr=pbsmgr(incldirs=options.dirs #AGAIN! i fail to input as a list!
    ,qselectfilter=' -u '+uid
    )
    
    
	pbsjobmonitor( mainpbsmgr
    ,by=options.by
	,attribstomon=options.watch
    , refreshtime=options.refresh )


