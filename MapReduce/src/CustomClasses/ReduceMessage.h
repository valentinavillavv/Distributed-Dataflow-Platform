//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
// 

#ifndef CUSTOMCLASSES_REDUCEMESSAGE_H_
#define CUSTOMCLASSES_REDUCEMESSAGE_H_

class ReduceMessage: public omnetpp::cMessage{
    protected:
        int module;
        //this is the numbers of workers active when the reduce phase starts in the coordinator, even if one of the workers fails during the execution of
        //the reduce phase, we always need the number of active workers when the reduce phase started, since we need to compute key%workers = modulo
        //and we cannot change in runtime how keys are associated to each worker (we need to be able to get also the tuples associated to the worker that failed)
        int workers;
    public:
        ReduceMessage(int m, int w, const char* name=NULL, short kind=0):cMessage(name,kind){
           module = m;
           workers = w;
        }

        virtual ~ReduceMessage() {}

        void setModule(int o){
            module=o;
        }

        int getModule(){
            return module;
        }

        int getWorkers(){
            return workers;
        }
};

#endif /* CUSTOMCLASSES_REDUCEMESSAGE_H_ */
