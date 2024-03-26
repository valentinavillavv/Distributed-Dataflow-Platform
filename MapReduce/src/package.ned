package mapreduce;

@license(LGPL);

simple Worker
{
    parameters:
        @display("i=device/mainframe");
    gates:
        //gates for coordinator
        output out;
        input in;
        output out_mem;
        input in_mem;
}

simple Coordinator
{
    parameters:
          @display("i=device/antennatower");
    gates:
        input in[]; //we need N connections coming from N workers
        output out[]; //we need N connections to N workers
        output out_mem;
        input in_mem;
}

//channel does not need a C++ class, since it enherits all the info from her father
channel MessageChannels extends ned.DatarateChannel{
    //we can also change others paramethers like datarate..
    delay = 100ms;
} 

simple DataStorage
{
	parameters:
	    @display("i=device/server");
	gates:
	    input in[];
	    output out[];
	    input in_coord;
	    output out_coord; 
}

network MapReduce
{
    parameters:
        int n_workers = default(8);
    submodules:
        workers[n_workers]: Worker;
        coordinator: Coordinator;
        datastore: DataStorage;
    connections:
        coordinator.out_mem --> MessageChannels --> datastore.in_coord;
        datastore.out_coord --> MessageChannels --> coordinator.in_mem;
        for i = 0..n_workers-1{
            //connections between workers and coordinator
            coordinator.out++ --> MessageChannels --> workers[i].in;
            datastore.out++ --> MessageChannels --> workers[i].in_mem;
            workers[i].out --> MessageChannels --> coordinator.in++;
            workers[i].out_mem --> MessageChannels --> datastore.in++;
        }
}