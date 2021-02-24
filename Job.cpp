

#include "Job.h"

Job::Job(const MapReduceClient &client1,
         const InputVec &inputVec1, OutputVec& outputVec1,
         int multiThreadLevel1,Barrier* barrier1, sem_t sem1, pthread_t *pthread, Context *context):
         atomic_counter(0),all(0)
 {
    inputVec = &inputVec1;

    mutex = PTHREAD_MUTEX_INITIALIZER;
    reduce_mutex = PTHREAD_MUTEX_INITIALIZER;
    output_mutex = PTHREAD_MUTEX_INITIALIZER;
    client = &client1;
    multyThreadLevel = multiThreadLevel1;
    outputVec = &outputVec1;
    barrier = barrier1;
    jobState.stage = UNDEFINED_STAGE;
    jobState.percentage = 0;
    sem = sem1;
    finish_shuffle = false;
    threads = pthread;
    sum_key = 0;
    wasInWait= false;
    contexts = context;
}

Job::~Job() {
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&reduce_mutex);
    pthread_mutex_destroy(&output_mutex);
    sem_destroy(&sem);
    delete barrier;
    delete []threads;
    delete []contexts;

}
