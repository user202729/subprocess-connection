#!/bin/python
import sys
from subprocess_connection import Connection, Message

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

	elif sys.argv[1]=="1":
		# parent process (test 2 -- Message)
		import subprocess
		message=Message(subprocess.Popen([sys.executable, __file__, "2"],
			stdin=subprocess.PIPE,
			stdout=subprocess.PIPE,
			))
		message.start()
		print(message.func.sqrt(5))
		message.stop()

	elif sys.argv[1]=="2":
		# child process (test 2 -- Message)
		message=Message()
		from time import sleep
		def sqrt(x):
			print("before sleep")
			sleep(1)
			print("after sleep")
			return x**.5
		message.func.sqrt=sqrt
		message.exec_()

	else:
		assert False


