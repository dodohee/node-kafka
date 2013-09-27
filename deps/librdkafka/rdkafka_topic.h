/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012,2013 Magnus Edenhill
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

void rd_kafka_toppar_destroy0 (rd_kafka_toppar_t *rktp);
void rd_kafka_toppar_insert_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_enq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);
void rd_kafka_toppar_deq_msg (rd_kafka_toppar_t *rktp, rd_kafka_msg_t *rkm);


#define rd_kafka_topic_keep(rkt)  rd_atomic_add(&(rkt->rkt_refcnt), 1)
void rd_kafka_topic_destroy0 (rd_kafka_topic_t *rkt);

void rd_kafka_toppar_broker_delegate (rd_kafka_toppar_t *rktp,
				      rd_kafka_broker_t *rkb);

void rd_kafka_topic_update (rd_kafka_t *rk,
			    const char *topic, int32_t partition,
			    int32_t leader);

void rd_kafka_topic_assign_uas (rd_kafka_t *rk, const char *topic);


void rd_kafka_topic_partition_cnt_update (rd_kafka_t *rk,
					  const char *topic,
					  int32_t partition_cnt);
