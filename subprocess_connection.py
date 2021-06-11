import sys
import io
from typing import Optional, Any, BinaryIO, Callable, Dict, Union
from threading import Lock, Thread
from subprocess import Popen
import pickle
from queue import Queue

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
			self._send_pipe=process.stdin  # type: ignore
			self._recv_pipe=process.stdout  # type: ignore
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


_FUNCTION: str="_FUNCTION"
_FUNCTION_RESPONSE: str="_FUNCTION_RESPONSE"

class Message:
	def __init__(self, x: Union[Connection, Popen]=None)->None:
		if x is None:
			connection=Connection()
		elif isinstance(x, Connection):
			connection=x
		else:
			connection=Connection(x)
		self._connection: Connection=connection
		self._calls: Dict[Any, Callable]={}
		self._funcs: Dict[Any, Callable]={}

		message: Message=self

		class _CallProxy:
			def __setitem__(self, key: Any, value: Callable)->None:
				message.set_call(key, value)

			def __setattr__(self, key: Any, value: Callable)->None:
				message.set_call(key, value)

			def __getitem__(self, key: Any)->Callable:
				return lambda *args, **kwargs: message.call_remote(key, args, kwargs)

			def __getattr__(self, key: Any)->Callable:
				return lambda *args, **kwargs: message.call_remote(key, args, kwargs)


		class _FuncProxy:
			def __setitem__(self, key: Any, value: Callable)->None:
				message.set_func(key, value)

			def __setattr__(self, key: Any, value: Callable)->None:
				message.set_func(key, value)

			def __getitem__(self, key: Any)->Callable:
				return lambda *args, **kwargs: message.func_remote(key, args, kwargs)

			def __getattr__(self, key: Any)->Callable:
				return lambda *args, **kwargs: message.func_remote(key, args, kwargs)

		self._call_proxy=_CallProxy()
		self._func_proxy=_FuncProxy()
		self._func_response_queues: Dict[int, Queue]={}
		self._response_counter: int=0
		self._response_counter_lock: Lock=Lock()

		self._exec_running: bool=False
		self._exec_running_lock: Lock=Lock()

		self._exec_thread: Thread=Thread(target=self.exec_)

		self.call[_FUNCTION]=self._on_func_called
		self.call[_FUNCTION_RESPONSE]=self._on_func_response

	def set_call(self, key: Any, value: Callable)->None:
		self._calls[key]=value

	def set_func(self, key: Any, value: Callable)->None:
		self._funcs[key]=value

	def exec_(self, suppress_call_errors: bool=True)->None:
		"""
		Listen for and execute requests.
		Usually this is run in a separate thread, but it's not required.
		"""
		with self._exec_running_lock:
			if self._exec_running:
				raise RuntimeError("exec_ is running")
			self._exec_running=True
		
		try:
			while True:
				try:
					data=self._connection.recv()
				except EOFError:
					break

				if data is None:
					self.stop()
					break
				key, args, kwargs=data

				try:
					self._calls[key](*args, **kwargs)
				except:
					if suppress_call_errors:
						import traceback
						traceback.print_exc()
					else:
						raise
		finally:
			with self._exec_running_lock:
				self._exec_running=False

	def start(self)->None:
		"""
		Run self.exec_() in another thread.

		It must be called at most once per Message object.
		"""
		self._exec_thread.start()

	def stop(self)->None:
		"""
		Stop the exec thread of both processes.

		This will only work if both processes have `exec_` running.
		"""
		self._connection.send(None)

	@property
	def call(self)->Any:
		return self._call_proxy

	@property
	def func(self)->Any:
		return self._func_proxy

	def call_remote(self, key: Any, args: tuple, kwargs: dict)->None:
		self._connection.send((key, args, kwargs))

	def func_remote(self, key: Any, args: tuple, kwargs: dict)->Any:
		with self._response_counter_lock:
			response_counter=self._response_counter=self._response_counter+1
		with self._exec_running_lock:
			if not self._exec_running:
				raise RuntimeError("exec_ is not running")
		assert response_counter not in self._func_response_queues
		self._func_response_queues[response_counter]=Queue(1)
		self.call_remote(_FUNCTION, (key, args, kwargs, response_counter), {})
		result: Any=self._func_response_queues[response_counter].get()
		del self._func_response_queues[response_counter]
		return result

	def _on_func_called(self, key: Any, args: list, kwargs: dict, response_counter: int)->None:
		result=self._funcs[key](*args, **kwargs)
		self.call_remote(_FUNCTION_RESPONSE, (response_counter, result), {})

	def _on_func_response(self, response_counter: Any, result: Any)->None:
		self._func_response_queues[response_counter].put(result)
