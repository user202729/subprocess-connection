#!/bin/python
import sys
from subprocess_connection import Connection, Message

if len(sys.argv)==1:
	# test 0, parent process
	# to call: python test.py
	import subprocess
	connection=Connection(subprocess.Popen([sys.executable, __file__, "0_child"],
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE,
		))
	connection.send(2)
	print("1 ==", connection.recv())

else:
	assert len(sys.argv)==2

	if sys.argv[1]=="0_child":
		# child process
		connection=Connection()
		connection.send(1)
		print("2 ==", connection.recv())

	elif sys.argv[1]=="1":
		# parent process (test 2 -- Message)
		# to call: python test.py 1
		# expected output:
		"""
		before sleep
		after sleep
		2.23606797749979

		before sleep 2
		after sleep 2
		before sleep 3
		# ^^^ ||| vvv      -- interchangeable
		2.23606797749979
		done

		"""

		import subprocess
		message=Message(subprocess.Popen([sys.executable, __file__, "1_child"],
			stdin=subprocess.PIPE,
			stdout=subprocess.PIPE,
			))
		message.start()
		print(message.func.sqrt(5))
		print(message.func.sqrt_1(5))
		message.stop()

	elif sys.argv[1]=="1_child":
		# child process (test 2 -- Message)
		message=Message()
		from time import sleep

		@message.register_func
		def sqrt(x):
			print("before sleep")
			sleep(1)
			print("after sleep")
			return x**.5
		#message.func.sqrt=sqrt

		@message.register_func_with_callback
		def sqrt_1(callback, args: tuple, kwargs: dict)->None:
			x,=args

			print("before sleep 2")
			sleep(1)
			print("after sleep 2")
			callback(x**.5)
			print("before sleep 3")
			sleep(1)
			print("done")

		message.exec_()

	else:
		assert False


