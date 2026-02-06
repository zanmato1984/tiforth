include_guard(GLOBAL)

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

enable_testing()

function(add_tiforth_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(
    ${TEST_NAME}
    PRIVATE tiforth GTest::gtest GTest::gtest_main Threads::Threads ArrowTesting::arrow_testing broken_pipeline::schedule)
  target_include_directories(${TEST_NAME} PRIVATE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>)
  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()
