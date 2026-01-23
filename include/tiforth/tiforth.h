#pragma once

#define TIFORTH_VERSION_MAJOR 0
#define TIFORTH_VERSION_MINOR 1
#define TIFORTH_VERSION_PATCH 0

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/spill.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/pipeline_context.h"
#include "tiforth/pipeline/task_groups.h"
#include "tiforth/task/blocked_resumer.h"
#include "tiforth/task/task_context.h"
#include "tiforth/task/task_group.h"
#include "tiforth/task/task_status.h"
#include "tiforth/type_metadata.h"

#include "tiforth/operators/filter.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/operators/projection.h"
#include "tiforth/operators/sort.h"
