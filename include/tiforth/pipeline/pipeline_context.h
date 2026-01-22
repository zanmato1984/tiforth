#pragma once

#include "tiforth/query_context.h"

namespace tiforth::pipeline {

struct PipelineContext {
  const ::tiforth::QueryContext* query_ctx = nullptr;
};

}  // namespace tiforth::pipeline

