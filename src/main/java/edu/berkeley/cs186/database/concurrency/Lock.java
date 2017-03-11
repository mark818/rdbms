package edu.berkeley.cs186.database.concurrency;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;
import java.lang.*;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


  private Set<Long> transactionOwners;
  private ConcurrentLinkedQueue<LockRequest> transactionQueue;
  private LockManager.LockType type;

  public Lock(LockManager.LockType type) {
    this.transactionOwners = new HashSet<Long>();
    this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
    this.type = type;
  }

  protected Set<Long> getOwners() {
    return this.transactionOwners;
  }

  public LockManager.LockType getType() {
    return this.type;
  }

  private void setType(LockManager.LockType newType) {
    this.type = newType;
  }

  public int getSize() {
    return this.transactionOwners.size();
  }

  public boolean isEmpty() {
    return this.transactionOwners.isEmpty();
  }

  private boolean containsTransaction(long transNum) {
    return this.transactionOwners.contains(transNum);
  }

  private void addToQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.add(lockRequest);
  }

  private void removeFromQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.remove(lockRequest);
  }

  private void addOwner(long transNum) {
    this.transactionOwners.add(transNum);
  }

  private void removeOwner(long transNum) {
    this.transactionOwners.remove(transNum);
  }

  private boolean checkCompatible(long transNum, LockManager.LockType lockType) {
    //System.out.print(Thread.currentThread().getName() + " owner has " + transactionOwners.size());
    LockRequest top = transactionQueue.peek();
    LockRequest r = new LockRequest(transNum, lockType);
    if (transactionOwners.size() == 0 && top.equals(r)) {
      //System.out.println("empty owner im at the top true");
      return true;
    }
    if (transactionOwners.contains(transNum)) {
      if (type == LockManager.LockType.EXCLUSIVE) {
        //System.out.println("Same tranx same lock");
        return true;
      }
      if (transactionOwners.size() == 1 &&
          type == LockManager.LockType.SHARED && lockType == LockManager.LockType.EXCLUSIVE) {
        //System.out.println("upgrade true");
        return true;
      }
    }
    if ((type != LockManager.LockType.SHARED) || (lockType != LockManager.LockType.SHARED)) {
      //System.out.println("not all SHARED false");
      return false;
    }

    if (r.equals(top)) {
      //System.out.println("at the top true");
      return true;
    }
    Iterator<LockRequest> iter = transactionQueue.iterator();
    while (iter.hasNext()) {
      LockRequest el = iter.next();
      if (el.equals(r)) {
        //System.out.println("found self true");
        return true;
      } else if (el.getType() == LockManager.LockType.EXCLUSIVE) {
        //System.out.println("EXCLUSIVE ahead false");
        return false;
      }
    }
    return true;
  }

  /**
   * Attempts to resolve the specified lockRequest. Adds the request to the queue
   * and calls wait() until the request can be promoted and removed from the queue.
   * It then modifies this lock's owners/type as necessary.
   * @param transNum transNum of the lock request
   * @param lockType lockType of the lock request
   */
  protected synchronized void acquire(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    LockRequest r = new LockRequest(transNum, lockType);
    transactionQueue.add(r);
    while (!checkCompatible(transNum, lockType)) {
      try {
        wait();
        //System.out.println(Thread.currentThread().getName() + " waked");
      } catch (Exception e) {
        //System.out.println(e.getClass().toString() + " " + e.getMessage());
      }
    }
    transactionQueue.remove(r);
    addOwner(transNum);
    type = lockType;
  }

  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   */
  protected synchronized void release(long transNum) {
    //TODO: Implement Me!!
    removeOwner(transNum);
    type = LockManager.LockType.SHARED;
    //System.out.println("Release, remaning owner " + transactionOwners.size());
    notifyAll();
    return;
  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    return (transactionOwners.contains(transNum) && type == lockType);
  }

  /**
   * LockRequest objects keeps track of the transNum and lockType.
   * Two LockRequests are equal if they have the same transNum and lockType.
   */
  private class LockRequest {
      private long transNum;
      private LockManager.LockType lockType;
      private LockRequest(long transNum, LockManager.LockType lockType) {
        this.transNum = transNum;
        this.lockType = lockType;
      }

      @Override
      public int hashCode() {
        return (int) transNum;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof LockRequest))
          return false;
        if (obj == this)
          return true;

        LockRequest rhs = (LockRequest) obj;
        return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
      }

      public LockManager.LockType getType() {
        return lockType;
      }
  }

}
