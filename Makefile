ubuntu_uv_activate:
	source venv/ubuntu_uv/bin/activate

uv_requirements:
	uv pip sync requirements.linux.txt

uv_install:
	uv pip install $(package)

ubuntu_activate:
	source venv/ubuntu/bin/activate

windows_activate:
	cd venv/windows/Scripts/ && activate && cd ../../