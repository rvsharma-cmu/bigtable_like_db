# Welcome to Big table Implementation

### Testing

1. First start the master server on any host. `make master`
2. Then start tablet servers using `make tablet1`, `make tablet2` and `make tablet3` command  

If command line reports module not found error, in the starter/ directory:
1. Symlink to current working directory `python3 -m venv $(pwd)`
2. `source ./bin/activate`
3. `pip3 install flask` and `pip3 install requests`
4. Try running make commands again

- make commands for master and tablet will modify the hostname as IP address of the corresponding server in `hosts.mk` file.
- We have assumed that all tablets and master run on different servers 

### Implementation details

- `master_server.py` has the master server implementation. The APIs implemented are listed in `doc/master.md`. 
- `tablet_server.py` has the tablet server implementation.  The APIs for tablet server are listed in `doc/tablet.md`
- `dataset` has the test dataset 

### Authors: 
@rvsharma-cmu
@vasudevluthra
