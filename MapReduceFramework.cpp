#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include "Job.h"
#include <algorithm>
#include <stdlib.h>
#include <iostream>

using namespace std;


/*
 * creates a mutex and checks if it's working
 * @param pmutex the name of the mutex
 */
void mutex_and_check(pthread_mutex_t *pmutex)
{
    if (pthread_mutex_lock(pmutex) != 0){ //should add * to
        fprintf(stderr, "Error: error on pthread_mutex_lock");
        exit(1);
    }
}


/*
 * unlocks the mutex and checks if it's unlock
 * @param pmutex
 */
void unmutex_and_check(pthread_mutex_t *pmutex)
{
    if (pthread_mutex_unlock(pmutex) != 0) {
        fprintf(stderr, "Error: error on pthread_mutex_unlock");
        exit(1);
    }
}


/**
 * gets a pair and entered it into intermidateVec
 * @param key the key we should enter to the vec
 * @param value the value we should enter to the vec
 * @param context the context were all the keys and values supposed to saved in.
 */
void emit2(K2* key, V2* value, void* context){
    Context* context1 = (Context*)context;
    IntermediatePair pair1(key,value);
    context1->intermediateVec->push_back(pair1);

}


/**
 * gets a pair and entered it into outputVec
 * @param key the key we should enter to the vec
 * @param value the value we should enter to the vec
 * @param context the context were all the keys and values supposed to saved in.
 */
void emit3 (K3* key, V3* value, void* context){
    Context* context1 = (Context*) context;
    OutputPair pair(key, value);
    mutex_and_check(&context1->job->output_mutex);
    context1->job->outputVec->push_back(pair);
    unmutex_and_check(&context1->job->output_mutex);
}


/*
 * map the threads based on the client's map function. and calculates the percentages of map
 * @param context where all the job is saved in.
 */
void mapThread(Context* context){
    while(true){
        int pairId = context->job->atomic_counter++;
        if(pairId >= (int) context->job->inputVec->size()){
            break;

        }
        InputPair pair1 = context->job->inputVec->at(pairId);
        context->job->client->map(pair1.first, pair1.second, context);
        context->job->precent++;
        context->job->jobState.percentage = (((float) context->job->precent/  (float)context->job->inputVec->size())*100);
    }
}


/*
 * finds the max key in all the keys in the context
 * @param context all the keys we checks in saved there
 * @return pair with the max key.
 */
IntermediatePair findingMax(Context* context){
    IntermediatePair maxPair;
    bool first = true;
    for(int i=0; i<(int) context->job->all.size(); i++){
        if(context->job->all[i].empty()){
            continue;
        }
        if(first){
            first= false;
            maxPair = context->job->all.at(i).back();
        }
        if(!context->job->all[i].empty()){
            if(*maxPair.first < *context->job->all[i].back().first){
                maxPair = context->job->all.at(i).back();
            }

        }
    }
    return maxPair;
}


/*
 * create new sequences of (k2,v2) where in each sequence all keys are identical and all elements with a given key are
 * in a single sequence.
 * @param context where all the job is saved
 */
void shuffle(Context* context){
    for(unsigned int i=0; i<context->multiThreadLevel; i++){
        context->job->all.push_back(*(context->job->contexts[i].intermediateVec));
    }
    for(unsigned int j = 0; j<context->job->all.size(); j++){
        if(!context->job->all.at(j).empty()){
            context->job->sum_key += context->job->all.at(j).size();
        }
    }
    while(true){
        bool isEmpty= true;
        for(int i = 0;i<(int) context->job->all.size(); i++){
            if(!context->job->all.at(i).empty()){
                isEmpty= false;
            }
        }
        if (isEmpty){
            break;
        }
        IntermediatePair maxkey = findingMax(context);
        vector<IntermediatePair> newVec;
        for(unsigned int i=0; i<context->job->all.size() ;i++){
            while (true){
                if(context->job->all.at(i).empty()){
                    break;
                }
                if(*maxkey.first < *context->job->all.at(i).back().first || *context->job->all.at(i).back().first < *maxkey.first){
                    break;
                } else{
                    newVec.push_back(context->job->all.at(i).back());
                    context->job->all.at(i).pop_back();
                }
            }
        }
       if(!newVec.empty()){
            mutex_and_check(&context->job->reduce_mutex);
            context->job->toReduce.push_back(newVec);
            unmutex_and_check(&context->job->reduce_mutex);
            sem_post(&context->job->sem);
        }
    }
    mutex_and_check(&context->job->reduce_mutex);
    context->job->finish_shuffle = true;
    unmutex_and_check(&context->job->reduce_mutex);
}


/*
 * wait for the shuffled vectors to be created by the shuffling thread and runs the client's reduce
 * @param context where all the job is saved in.
 */
void reduceThred(Context* context){
    while (true){
        mutex_and_check(&context->job->reduce_mutex);
        if(context->job->finish_shuffle && context->job->toReduce.empty()){
            unmutex_and_check(&context->job->reduce_mutex);
            break;
        }
        unmutex_and_check(&context->job->reduce_mutex);
        sem_wait(&context->job->sem);
        mutex_and_check(&context->job->reduce_mutex);

        if(context->job->toReduce.empty()){
            unmutex_and_check(&context->job->reduce_mutex);
            continue;
        } else{
            unmutex_and_check(&context->job->reduce_mutex);
            mutex_and_check(&context->job->reduce_mutex);
            for(int i=0; i<(int) context->job->toReduce.back().size(); i++){
                context->job->reduced++;
            }
            context->job->client->reduce(&context->job->toReduce.back(), context);
            context->job->toReduce.pop_back();
            unmutex_and_check(&context->job->reduce_mutex);
            context->job->jobState.percentage = (((float)context->job->reduced /(float) context->job->sum_key)*100);
        }
    }
    sem_post(&context->job->sem);
}


bool compFunc(IntermediatePair i, IntermediatePair j){
    return (*(i.first) < *(j.first));
}


/*
 * the func each thread should run
 */
void* foo(void* arg){
    Context* context = (Context*) arg;
    context->job->jobState.stage = MAP_STAGE;
    mapThread(context);
    std::sort((context->intermediateVec)->begin(), context->intermediateVec->end(), compFunc);
    context->job->barrier->barrier();
    if( context->job->jobState.stage == MAP_STAGE){
        context->job->jobState.stage = REDUCE_STAGE;
        context->job->jobState.percentage = 0;
    }
    if(context->id == 0){
        shuffle(context);
    }
    reduceThred(context);
    delete (context->intermediateVec);
    return 0;
}


/**
 * This function starts running the MapReduce algorithm (with several threads) and returns a JobHandle.
 * @param client The implementation of MapReduceClient or in other words the task that the framework should run.
 * @param inputVec a vector of type std::vector<std::pair<K1*, V1*>>, the input elements.
 * @param outputVec a vector of type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added before returning. You can assume that outputVec is empty.
 * @param multiThreadLevel he number of worker threads to be used for running the algorithm. You can assume this argument is valid
 * @return struct with all the elements for a job.
 */
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel) {

    Barrier *barrier = new Barrier(multiThreadLevel);
    sem_t sem;
    pthread_t *threads = new pthread_t[multiThreadLevel];
    Context *contexts = new Context[multiThreadLevel];
    sem_init(&sem, 0, 0);
    Job *job = nullptr;
    job = new Job(client, inputVec, outputVec, multiThreadLevel, barrier, sem, threads, contexts);

    for (int i = 0; i < multiThreadLevel; ++i) {
        auto inter = new IntermediateVec();
        contexts[i] = {job, i, (unsigned int)multiThreadLevel,inter};
    }

    for (int i = 0; i < job->multyThreadLevel; ++i) {
        pthread_create(threads+i, NULL, foo, contexts + i);
    }
    return job;
}


/**
 * this function gets a job handle and check for his current state in a given JobState struct.
 */
void getJobState(JobHandle job, JobState* state){
    Job* job1 = (Job*) job;
    state->stage = job1->jobState.stage;
    state->percentage = job1->jobState.percentage;
}


/**
 * a function gets the job handle returned by startMapReduceJob and waits until it is finished.
 */
void waitForJob(JobHandle job){
    Job* job1 = (Job*) job;
    job1->wasInWait= true;
    for (int i = 0; i < job1->multyThreadLevel; ++i) {
        if(pthread_join((*(job1->threads+i)), NULL) != 0){
            std::cerr<<"problem in join func with thread %d\n"<<i<<std::endl;
        }
    }

}


/**
 * Releasing all resources of a job. You should prevent releasing resources before the job finished. After this function is called the job handle will be invalid.
 */
void closeJobHandle(JobHandle job){
    Job* job1 = (Job*) job;
    if(!job1->wasInWait){
        waitForJob(job1);
    }
    delete job1;
}
