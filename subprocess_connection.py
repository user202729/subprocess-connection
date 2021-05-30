import sys
import io
from typing import Optional, Any, BinaryIO
from threading import Lock
from subprocess import Popen
import pickle

class Connection:
	"""
	An object with interface similar to multiprocessing.Connection,
	but uses stdin and stdout.
	"""

	def __init__(self, process: Optional[Popen]=None, redirect_stdout=True)->None:
		self._process: Optional[Popen]=process
		self._send_pipe: Optional[BinaryIO]
		self._recv_pipe: Optional[BinaryIO]

		if process:
			#assert (
			#		process.stdin is None or isinstance(process.stdin, BinaryIO)
			#	) and (
			#		process.stdout is None or isinstance(process.stdout, BinaryIO)
			#	), "Subprocess pipes must be opened in binary mode!"
			self._send_pipe=process.stdin
			self._recv_pipe=process.stdout
		else:
			self._send_pipe=sys.stdout.buffer
			self._recv_pipe=sys.stdin.buffer
			if redirect_stdout:
				sys.stdout=sys.stderr

		self._send_lock: Optional[Lock]=None if self._send_pipe is None else Lock()
		self._recv_lock: Optional[Lock]=None if self._recv_pipe is None else Lock()

	@property
	def process(self)->Optional[Popen]:
		return self._process

	def close(self)->None:
		if self._send_pipe is not None:
			self._send_pipe.close()
		if self._recv_pipe is not None:
			self._recv_pipe.close()

	def __del__(self)->None:
		self.close()

	def send_bytes(self, buffer: bytes, offset: int=0, size: Optional[int]=None)->None:
		assert self._send_lock
		with self._send_lock:
			assert self._send_pipe

			assert 0<=offset<=len(buffer)
			if size is None:
				size=len(buffer)-offset

			self._send_pipe.write(size.to_bytes(8, "little"))
			self._send_pipe.write(buffer[offset:offset+size])
			self._send_pipe.flush()

	def _raw_read(self, size: int)->bytes:
		# not locked
		assert self._recv_pipe
		result=self._recv_pipe.read(size)
		if len(result)!=size:
			raise EOFError(f"Tried to read {size} bytes, only received {len(result)}")

		#retries=5
		#while len(result)!=size:
		#	retries-=1
		#	assert retries>=0, f"Tried to read {size} bytes, only received {len(result)}"
		#	result+=self._recv_pipe.read(size-len(result))
		return result

	def recv_bytes(self, maxlength: int=None):
		assert self._recv_lock
		with self._recv_lock:
			
			message_size: int = int.from_bytes(self._raw_read(8), 'little') # 8 is definitely enough
			if maxlength is not None and message_size>maxlength:
				self.close()
				raise OSError(f"Message too long: maxlength={maxlength}, message size={message_size}")
			return self._raw_read(message_size)

	def send(self, o: Any)->None:
		self.send_bytes(pickle.dumps(o))

	def recv(self)->Any:
		return pickle.loads(self.recv_bytes())

	def fileno(self)->Any:
		raise NotImplementedError

	def poll(self, timeout: Optional[int]=0)->bool:
		raise NotImplementedError

	def recv_bytes_into(self, buffer: bytearray, offset: int=0)->None:
		raise NotImplementedError



