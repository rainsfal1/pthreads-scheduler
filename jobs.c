
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "executor.h"

extern struct executor tassadar;


/**
 * Populate the job lists by parsing a file where each line has
 * the following structure:
 *
 * <id> <type> <num_resources> <resource_id_0> <resource_id_1> ...
 *
 * Each job is added to the queue that corresponds with its job type.
 */
void parse_jobs(char *file_name) {
    int id;
    struct job *cur_job;
    struct admission_queue *cur_queue;
    enum job_type jtype;
    int num_resources, i;
    FILE *f = fopen(file_name, "r");

    /* parse file */
    while (fscanf(f, "%d %d %d", &id, (int*) &jtype, (int*) &num_resources) == 3) {

        /* construct job */
        cur_job = malloc(sizeof(struct job));
        cur_job->id = id;
        cur_job->type = jtype;
        cur_job->num_resources = num_resources;
        cur_job->resources = malloc(num_resources * sizeof(int));

        int resource_id;
				for(i = 0; i < num_resources; i++) {
				    fscanf(f, "%d ", &resource_id);
				    cur_job->resources[i] = resource_id;
				    tassadar.resource_utilization_check[resource_id]++;
				}

				assign_processor(cur_job);

        /* append new job to head of corresponding list */
        cur_queue = &tassadar.admission_queues[jtype];
        cur_job->next = cur_queue->pending_jobs;
        cur_queue->pending_jobs = cur_job;
        cur_queue->pending_admission++;
    }

    fclose(f);
}

/*
 * Magic algorithm to assign a processor to a job.
 */
void assign_processor(struct job* job) {
    int i, proc = job->resources[0];
    for(i = 1; i < job->num_resources; i++) {
        if(proc < job->resources[i]) {
            proc = job->resources[i];
        }
    }
    job->processor = proc % NUM_PROCESSORS;
}


void do_stuff(struct job *job) {
    /* Job prints its id, its type, and its assigned processor */
    printf("%d %d %d\n", job->id, job->type, job->processor);
}



void init_executor() {
    int i, j;

    // Initialize resource locks
    for (i = 0; i < NUM_RESOURCES; i++) {
        pthread_mutex_init(&tassadar.resource_locks[i], NULL);
        tassadar.resource_utilization_check[i] = 0;
    }

    // Initialize admission queues
    for (i = 0; i < NUM_QUEUES; i++) {
        struct admission_queue *queue = &tassadar.admission_queues[i];

        pthread_mutex_init(&queue->lock, NULL);
        pthread_cond_init(&queue->admission_cv, NULL);
        pthread_cond_init(&queue->execution_cv, NULL);

        queue->pending_jobs = NULL;
        queue->pending_admission = 0;

        queue->capacity = QUEUE_LENGTH;
        queue->num_admitted = 0;
        queue->head = 0;
        queue->tail = 0;

        queue->admitted_jobs = malloc(QUEUE_LENGTH * sizeof(struct job *));
        for (j = 0; j < QUEUE_LENGTH; j++) {
            queue->admitted_jobs[j] = NULL;
        }
    }

    // Initialize processor records
    for (i = 0; i < NUM_PROCESSORS; i++) {
        struct processor_record *record = &tassadar.processor_records[i];

        record->completed_jobs = NULL;
        record->num_completed = 0;
        pthread_mutex_init(&record->lock, NULL);
    }
}


/**
 *
 * Handles an admission queue passed in through the arg (see the executor.c file).
 * Bring jobs into this admission queue as room becomes available in it.
 * As new jobs are added to this admission queue (and are therefore ready to be taken
 * for execution), the corresponding execute thread must become aware of this.
 *
 */
void *admit_jobs(void *arg) {
    struct admission_queue *q = arg;
    struct job *job;

    while (1) {
        pthread_mutex_lock(&q->lock);

        // Wait until there's a pending job and space in the queue
        while (q->pending_jobs == NULL || q->num_admitted >= q->capacity) {
            pthread_cond_wait(&q->admission_cv, &q->lock);
        }

        // Remove the job from the pending list
        job = q->pending_jobs;
        q->pending_jobs = job->next;
        job->next = NULL;
        q->pending_admission--;

        // Add the job to the admitted queue
        q->admitted_jobs[q->tail] = job;
        q->tail = (q->tail + 1) % q->capacity;
        q->num_admitted++;

        // Signal the execute thread that a new job is available
        pthread_cond_signal(&q->execution_cv);

        pthread_mutex_unlock(&q->lock);
    }

    return NULL;
}

/**
 *
 * Moves jobs from a single admission queue of the executor.
 * Jobs must acquire the required resource locks before being able to execute.
 *
 * Note: You do not need to spawn any new threads in here to simulate the processors.
 * When a job acquires all its required resources, it will execute do_stuff.
 * When do_stuff is finished, the job is considered to have completed.
 *
 * Once a job has completed, the admission thread must be notified since room
 * just became available in the queue. Be careful to record the job's completion
 * on its assigned processor and keep track of resources utilized.
 *
 * Note: No printf statements are allowed in your final jobs.c code,
 * other than the one from do_stuff!
 */
void *execute_jobs(void *arg) {
    struct admission_queue *q = arg;
    struct job *job;
    int i, all_resources_acquired;

    while (1) {
        pthread_mutex_lock(&q->lock);

        // Wait until there's an admitted job
        while (q->num_admitted == 0) {
            pthread_cond_wait(&q->execution_cv, &q->lock);
        }

        // Get the job from the front of the queue
        job = q->admitted_jobs[q->head];

        pthread_mutex_unlock(&q->lock);

        // Attempt to acquire all required resources
        all_resources_acquired = 1;
        for (i = 0; i < job->num_resources; i++) {
            if (pthread_mutex_trylock(&tassadar.resource_locks[job->resources[i]]) != 0) {
                // Failed to acquire a resource, release all acquired resources
                all_resources_acquired = 0;
                int j;
                for (j = 0; j < i; j++) {
                    pthread_mutex_unlock(&tassadar.resource_locks[job->resources[j]]);
                }
                break;
            }
        }

        if (all_resources_acquired) {
            // Execute the job
            do_stuff(job);

            // Release all resources
            for (i = 0; i < job->num_resources; i++) {
                pthread_mutex_unlock(&tassadar.resource_locks[job->resources[i]]);
                tassadar.resource_utilization_check[job->resources[i]]--;
            }

            // Record job completion
            pthread_mutex_lock(&tassadar.processor_records[job->processor].lock);
            job->next = tassadar.processor_records[job->processor].completed_jobs;
            tassadar.processor_records[job->processor].completed_jobs = job;
            tassadar.processor_records[job->processor].num_completed++;
            pthread_mutex_unlock(&tassadar.processor_records[job->processor].lock);

            // Remove job from admission queue
            pthread_mutex_lock(&q->lock);
            q->admitted_jobs[q->head] = NULL;
            q->head = (q->head + 1) % q->capacity;
            q->num_admitted--;

            // Notify admission thread that space is available
            pthread_cond_signal(&q->admission_cv);
            pthread_mutex_unlock(&q->lock);
        }
    }

    return NULL;
}