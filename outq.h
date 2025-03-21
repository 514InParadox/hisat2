/*
 * Copyright 2011, Ben Langmead <langmea@cs.jhu.edu>
 *
 * This file is part of Bowtie 2.
 *
 * Bowtie 2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Bowtie 2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Bowtie 2.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef OUTQ_H_
#define OUTQ_H_

#include <atomic>
#include <vector>
#include <iostream>

#include "assert_helpers.h"
#include "ds.h"
#include "sstring.h"
#include "read.h"
#include "threading.h"
#include "mem_ids.h"

/**
 * Encapsulates a list of lines of output.  If the earliest as-yet-unreported
 * read has id N and Bowtie 2 wants to write a record for read with id N+1, we
 * resize the lines_ and committed_ lists to have at least 2 elements (1 for N,
 * 1 for N+1) and return the BTString * associated with the 2nd element.  When
 * the user calls commit() for the read with id N, 
 */
class OutputQueue {

	static const size_t NFLUSH_THRESH = 8;

public:

	OutputQueue(
		OutFileBuf* obuf,
		bool reorder,
		size_t nthreads,
		bool threadSafe,
		TReadId rdid = 0) :
		obuf_(obuf),
		cur_(rdid),
		nstarted_(0),
		nfinished_(0),
		nflushed_(0),
		lines_(RES_CAT),
		started_(RES_CAT),
		finished_(RES_CAT),
		reorder_(reorder),
		threadSafe_(threadSafe),
        mutex_m()
	{
		assert(nthreads <= 1 || threadSafe);
		if (reorder_) {
			for (int i = 0; i < nthreads; ++i) {
				obufArr_.push_back(new OutFileBuf(("output" + std::to_string(i)).c_str(), false));
			}
		}
	}

	/**
	 * Caller is telling us that they're about to write output record(s) for
	 * the read with the given id.
	 */
	void beginRead(TReadId rdid, size_t threadId);
	
	/**
	 * Writer is finished writing to 
	 */
	void finishRead(const BTString& rec, TReadId rdid, size_t threadId);
	
	/**
	 * Return the number of records currently being buffered.
	 */
	size_t size() const {
		return lines_.size();
	}
	
	/**
	 * Return the number of records that have been flushed so far.
	 */
	TReadId numFlushed() const {
		return nflushed_;
	}

	/**
	 * Return the number of records that have been started so far.
	 */
	TReadId numStarted() const {
		return nstarted_;
	}

	/**
	 * Return the number of records that have been finished so far.
	 */
	TReadId numFinished() const {
		return nfinished_;
	}

	/**
	 * Write already-committed lines starting from cur_.
	 */
	void flush(bool force = false, bool getLock = true);

protected:

	OutFileBuf*     obuf_;
	std::vector<OutFileBuf*> obufArr_;
	std::atomic<TReadId>         cur_;
	std::atomic<TReadId>         nstarted_;
	std::atomic<TReadId>         nfinished_;
	std::atomic<TReadId>         nflushed_;
	EList<BTString> lines_;
	EList<bool>     started_;
	EList<bool>     finished_;
	bool            reorder_;
	bool            threadSafe_;
	MUTEX_T         mutex_m;
};

class OutputQueueMark {
public:
	OutputQueueMark(
		OutputQueue& q,
		const BTString& rec,
		TReadId rdid,
		size_t threadId) :
		q_(q),
		rec_(rec),
		rdid_(rdid),
		threadId_(threadId)
	{
		q_.beginRead(rdid, threadId);
	}
	
	~OutputQueueMark() {
		q_.finishRead(rec_, rdid_, threadId_);
	}
	
protected:
	OutputQueue& q_;
	const BTString& rec_;
	TReadId rdid_;
	size_t threadId_;
};

/**
 * hy:
 * 单线程：
 * 	不存在 reorder 问题
 * 多线程：
 * 	如果不允许 reorder，则直接对一个 Buf 写入即可；
 * 	如果允许 reorder，那么可以直接多线程写多个文件，最后合并。
 * 即：直接将原逻辑中的 reorder 部分从对一个 Buf 改成对多个 Buf 写即可。
 */

#endif
