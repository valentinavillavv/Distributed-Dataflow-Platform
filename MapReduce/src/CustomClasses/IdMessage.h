#ifndef CUSTOMCLASSES_IDMESSAGE_H_
#define CUSTOMCLASSES_IDMESSAGE_H_

class IdMessage: public omnetpp::cMessage{
    protected:
        int ID;
    public:
        IdMessage(int id, const char* name=NULL, short kind=0):cMessage(name,kind){
           ID = id;
        }

        virtual ~IdMessage() {}

        int getID(){
            return ID;
        }
};


#endif /* CUSTOMCLASSES_IDMESSAGE_H_ */
