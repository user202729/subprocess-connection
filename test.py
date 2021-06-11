#!/bin/python
import sys
from subprocess_connection import Connection

if len(sys.argv)==1:
	# parent process
	import subprocess
	connection=Connection(subprocess.Popen([sys.executable, __file__, "0"],
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE,
		))
	connection.send(2)
	print("1 ==", connection.recv())

else:
	assert len(sys.argv)==2

	if sys.argv[1]=="0":
		# child process
		connection=Connection()
		connection.send(1)
		print("2 ==", connection.recv())

	else:
		assert False


