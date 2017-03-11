package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.concurrency.TestDeadlockPrevention;
import edu.berkeley.cs186.database.concurrency.TestLockManager;
import edu.berkeley.cs186.database.datatypes.*;
import edu.berkeley.cs186.database.index.*;
import edu.berkeley.cs186.database.io.*;
import edu.berkeley.cs186.database.query.*;
import edu.berkeley.cs186.database.table.stats.*;
import edu.berkeley.cs186.database.table.*;

import org.junit.runner.RunWith;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runners.Suite.SuiteClasses;

import edu.berkeley.cs186.database.table.TestSchema;

/**
 * A test suite for student tests.
 *
 * DO NOT CHANGE ANY OF THIS CODE.
 */
@RunWith(Categories.class)
@IncludeCategory(StudentTestP3.class)
@SuiteClasses({
        TestBoolDataType.class,
        TestFloatDataType.class,
        TestIntDataType.class,
        TestStringDataType.class,
        TestBPlusTree.class,
        TestLRUCache.class,
        TestPage.class,
        TestPageAllocator.class,
        GroupByOperatorTest.class,
        JoinOperatorTest.class,
        OptimalQueryPlanJoinsTest.class,
        OptimalQueryPlanTest.class,
        QueryPlanTest.class,
        QueryPlanCostsTest.class,
        SelectOperatorTest.class,
        WhereOperatorTest.class,
        BoolHistogramTest.class,
        FloatHistogramTest.class,
        IntHistogramTest.class,
        StringHistogramTest.class,
        TableStatsTest.class,
        TestSchema.class,
        TestTable.class,
        TestDatabase.class,
        TestDatabaseQueries.class,
        TestLockManager.class,
        TestDeadlockPrevention.class
})
public class StudentTestSuiteP3 {}
