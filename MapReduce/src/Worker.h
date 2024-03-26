#ifndef WORKER_H_
#define WORKER_H_

using namespace omnetpp;
using namespace std;

class Worker: public cSimpleModule{
    protected:
        int ID;

        vector<vector<tuple<int,int>>> input_data; //vector of partitions  received in input from the memory
        vector<int> memoryIndeces; //vector of the indeces of memory of the partitions received


        //those are all parameters received from the Coordinator
        string operationToDo; //this is the "map(ADD(3))"
        string operand; //map changekey or reduce
        string operation; //ADD, SUB...
        int value; //argument of the operation

        //series of self messages and type of messages, to send with a casual time each type of message towards coordinator and memory
        cMessage * self_OperationReduce = nullptr;

        //those variables are for the output and to know when to send them to the memory
        cMessage * self_Operation;
        deque<vector<tuple<int,int>>> queuePartitions;


        //variables for heartbeats
        double pOfFailure = 0.05;
        cMessage* self_heartbeat;
        int maxHeartDelay = 4,  minHeartDelay = 1;
        bool failed = false;
        bool waitingForPartition = false; //this is used to handle the situation in which the workers fails when it is waiting for the tuples from the memory
        bool detectedFailureFromCoordinator = false;

        bool finished = false;

        cMessage * self_Restart;
        double pRestarting = 0.5;
        int maxRestart = 3, minRestart = 1;

        int forwardmaxDelay = 2, forwardminDelay = 1;
        int maxDelay = 5, minDelay = 2;


        virtual void handleMessage(cMessage *msg) override;
        virtual void initialize() override;
        int convertString(string str, int start, int offset);
        int choose_operand(string s, int v, int v2);
        void sendFailureOperation();
        vector<tuple<int,int>> performOperation(vector<tuple<int,int>> input_data, int memoryIndex);
        vector<tuple<int,int>> perfromReduceOperation(vector<tuple<int,int>> partition);
        void typeOfOperation(string operationToDo);
        int randomNum(int max, int min);

    public:
        void cancelMessages();

        //variables for statistics
        int counterFailures = 0;
        int n_messages = 0;

        simtime_t TOTfailuretime;
        simtime_t beginfailure;
};

#endif /* WORKER_H_ */
