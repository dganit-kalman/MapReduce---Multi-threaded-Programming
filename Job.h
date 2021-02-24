
#include <atomic>
#include "MapReduceClient.h"
#include "Barrier.h"
#include "MapReduceFramework.h"
#include <semaphore.h>
#include <pthread.h>


struct Context;


class Job{
public:
    const InputVec* inputVec;
    OutputVec* outputVec;
    const MapReduceClient* client;
    int multyThreadLevel;
    std::atomic<int> atomic_counter;
    std::atomic<int> precent;
    std::atomic<int> reduced;
    Barrier* barrier;
    std::vector<IntermediateVec> all;
    std::vector<IntermediateVec> toReduce;
    JobState jobState;
    sem_t sem;
    pthread_mutex_t mutex;
    pthread_mutex_t reduce_mutex;
    pthread_mutex_t output_mutex;
    bool wasInWait;
    pthread_t *threads;
    int sum_key;
    bool finish_shuffle;
    Context *contexts;

    Job(const MapReduceClient &client1,
        const InputVec &inputVec1, OutputVec &outputVec1,
        int multiThreadLevel1,Barrier* barrier1, sem_t sem1, pthread_t *pthread,Context *context);
    ~Job();
};


 struct Context{
    Job* job;
    int id;
    unsigned int multiThreadLevel;
    IntermediateVec *intermediateVec;

};
