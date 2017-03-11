package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.experimental.categories.Category;
import edu.berkeley.cs186.database.StudentTestP3;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestDeadlockPrevention {
  private static final String TestDir = "testDatabase";
  private Database db;
  private String filename;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public Timeout maxGlobalTimeout = Timeout.seconds(10); // 10 seconds max per method tested


  @Before
  public void beforeEach() throws IOException, DatabaseException {
    File testDir = tempFolder.newFolder(TestDir);
    this.filename = testDir.getAbsolutePath();
    this.db = new Database(filename);
    this.db.deleteAllTables();
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  @Test
  public void testNoCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  @Category(StudentTestP3.class)
  public void testNonCircularDependency() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread2.start();
      thread3.start();
      thread4.start();
      thread5.start();
      
      assertFalse("Transaction 1 thread 1 should have finished", thread1.isAlive());
      assertTrue("Transaction 2 thread 2 should be waiting", thread2.isAlive());
      assertTrue("Transaction 3 thread 3 should be waiting", thread3.isAlive());
      assertFalse("Transaction 3 thread 4 should have finished", thread4.isAlive());
      assertTrue("Transaction 2 thread 5 should have finished", thread5.isAlive());

      lockMan.releaseLock("A", 1);
      thread2.join(200);
      assertFalse("Transaction 2 thread 2 should have finished", thread2.isAlive());

      lockMan.releaseLock("A", 2);
      thread3.join(200);
      assertFalse("Transaction 3 thread 3 should have finished", thread3.isAlive());

      lockMan.releaseLock("B", 3);
      thread5.join(200);
      assertFalse("Transaction 2 thread 5 should have finished", thread5.isAlive());

    } catch (DeadlockException e) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }
  }

// 1->3 2->1 3->2

//  A  1 (2)
//  B  2 (3)
//  C  3 (1)

  @Test
  @Category(StudentTestP3.class)
  public void testTriDependencyDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread0 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread 1");
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("C", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("A", 1);
        lockMan.releaseLock("C", 1);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("B", 2);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread 2");

    try {
      thread0.start();
      thread0.join(200);
      thread0.test();
      thread1.start();
      thread1.join(200);
      thread1.test();
      thread2.start();
      thread2.join(200);
      thread2.test();
      thread3.start();
      thread3.join(200);
      thread3.test();
      fail("Deadlock exists but DeadlockException not thrown");
    } catch (DeadlockException e) {
      //System.out.println("Caught expected DeadlockException");
    } finally {
      lockMan.releaseLock("C", 3);
      Thread.sleep(3000);
      assertFalse("Thread 1 should have finished", thread1.isAlive());
      assertFalse("Thread 2 should have finished", thread2.isAlive());
      assertFalse("Thread 3 should have finished", thread3.isAlive());
    }
  }

// 1 -> 2
// 4 <- 3
//  A  1 (2)
//  B  2 (3)
//  C  3 (4)
//  D  4 (1)

  @Test
  @Category(StudentTestP3.class)
  public void testQuadDependencyDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread0 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("D", 4, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 4 Thread 1");
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("D", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("A", 1);
        lockMan.releaseLock("D", 1);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("B", 2);
        lockMan.releaseLock("A", 2);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("C", 3);
        lockMan.releaseLock("B", 3);
      }
    }, "Transaction 3");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 4 Thread 2");

    try {
      thread0.start();
      thread0.join(200);
      thread0.test();
      thread1.start();
      thread1.join(200);
      thread1.test();
      thread2.start();
      thread2.join(200);
      thread2.test();
      thread3.start();
      thread3.join(200);
      thread3.test();
      thread4.start();
      thread4.join(200);
      thread4.test();
    } catch (DeadlockException e) {
      fail("No deadlock exists but DeadlockException thrown");
    } finally {
      lockMan.releaseLock("D", 4);
      Thread.sleep(3000);
      assertFalse("Thread 1 should have finished", thread1.isAlive());
      assertFalse("Thread 2 should have finished", thread2.isAlive());
      assertFalse("Thread 3 should have finished", thread3.isAlive());
      assertFalse("Thread 4 should have finished", thread4.isAlive());
    }
  }

  @Test
  @Category(StudentTestP3.class)
  public void testNotDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("C", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("A", 1);
        lockMan.releaseLock("C", 1);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("B", 2);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread 2");

    try {
      thread1.start();
      thread1.join(200);
      thread1.test();
      thread2.start();
      thread2.join(200);
      thread2.test();
      thread3.start();
      thread3.join(200);
      thread3.test();
    } catch (DeadlockException e) {
      fail("No deadlock exists but DeadlockException thrown");
    }
    assertFalse("Thread 1 shold have finished", thread1.isAlive()); 
    assertFalse("Thread 2 shold have finished", thread2.isAlive()); 
    assertFalse("Thread 3 shold have finished", thread3.isAlive()); 
  }

  @Test
  @Category(StudentTestP3.class)
  public void testPriorityNoDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
        lockMan.releaseLock("A", 1);
      }
    }, "Transaction 1 Thread 2");

    try {
      thread1.start();
      thread1.join(200);
      thread1.test();
      thread2.start();
      thread2.join(200);
      thread2.test();
      thread3.start();
      thread3.join(500);
      thread3.test();
    } catch (DeadlockException e) {
      fail("No deadlock exists but DeadlockException thrown");
    }
    assertFalse("Thread 1 shold have finished", thread1.isAlive()); 
    assertFalse("Thread 2 shold have finished", thread2.isAlive()); 
    assertFalse("Thread 3 shold have finished", thread3.isAlive()); 
  }


  @Test
  public void testNoDirectedCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 3 Thread should not have finished", thread3.isAlive()); //T3 should be waiting on T1 for A

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread4.isAlive()); //T2 should not be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertTrue("Transaction 3 Second Thread should not have finished", thread5.isAlive()); //T3 should be waiting on T2 for B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  public void testTwoTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread4.start();
      thread4.join(100);
      thread4.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertFalse("Transaction 3 Second Thread should have finished", thread5.isAlive()); //T3 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread6.start();
      thread6.join(100);
      thread6.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock2() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Third Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread7 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked on A

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should not be blocked on B

      thread4.start();
      thread4.join(100); //waits for thread to finish (timeout of .1 sec)
      thread4.test();
      assertTrue("Transaction 1 Second Thread should not have finished", thread4.isAlive()); //T1 should be waiting on T3 for B

      thread5.start();
      thread5.join(100); //waits for thread to finish (timeout of .1 sec)
      thread5.test();
      assertFalse("Transaction 1 Third Thread should have finished", thread5.isAlive()); //T1 should not be blocked on C

      thread6.start();
      thread6.join(100); //waits for thread to finish (timeout of .1 sec)
      thread6.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread6.isAlive()); //T2 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread7.start();
      thread7.join(100); //waits for thread to finish (timeout of .1 sec)
      thread7.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testNoSelfLoopsCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 2 Second Thread should not have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }


}
