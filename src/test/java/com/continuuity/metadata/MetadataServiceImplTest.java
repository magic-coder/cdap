package com.continuuity.metadata;

import com.continuuity.api.data.MetaDataException;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.stubs.Account;
import com.continuuity.metadata.stubs.MetadataServiceException;
import com.continuuity.metadata.stubs.Stream;
import com.continuuity.runtime.MetadataModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

/**
 * Tests metadata service functionality.
 */
public class MetadataServiceImplTest {
  /** Instance of operation executor */
  private static OperationExecutor opex;

  /** Instance of metadata service. */
  private static MetadataServiceImpl mds;

  /** Instance of account used for tests. */
  private static Account account;

  @BeforeClass
  public static void beforeMetadataService() throws Exception {
    Injector injector = Guice.createInjector(
      new MetadataModules().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules()
    );
    opex = injector.getInstance(OperationExecutor.class);
    mds = new MetadataServiceImpl(opex);
    account = new Account("demo");
  }

  @AfterClass
  public static void afterMetadataService() throws Exception {
    // nothing to be done here.
  }

  /**
   * Tests creation of streams with only Id. This should
   * throw MetadataServiceException.
   */
  @Test(expected = MetadataServiceException.class)
  public void testCreateStreamWithOnlyId() throws Exception {
    com.continuuity.metadata.stubs.Stream
        stream = new com.continuuity.metadata.stubs.Stream("id1");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
  }

  /**
   * Tests creation of streams with Id as empty string. This should
   * throw MetadataServiceException.
   */
  @Test(expected = MetadataServiceException.class)
  public void testCreateStreamWithEmptyId() throws Exception {
    com.continuuity.metadata.stubs.Stream
      stream = new com.continuuity.metadata.stubs.Stream("");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
  }

  /**
   * Tests creation of stream with only Id and Name. This should
   * throw MetadataServiceException.
   * @throws Exception
   */
  @Test(expected = MetadataServiceException.class)
  public void testCreateStreamWithIdAndName() throws Exception {
    com.continuuity.metadata.stubs.Stream
      stream = new com.continuuity.metadata.stubs.Stream("id1");
    stream.setName("Funny stream");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
  }

  /**
   * Tests creation of stream with all the necessary information.
   * This test should not throw any errors.
   * @throws Exception
   */
  @Test
  public void testCreateStreamCorrect() throws Exception {
    com.continuuity.metadata.stubs.Stream
      stream = new com.continuuity.metadata.stubs.Stream("id1");
    stream.setName("Funny stream");
    stream.setDescription("Funny stream that is so funny. You laugh it out");
    Assert.assertTrue(mds.createStream(account, stream));
    // Check if there is 1 stream available. Don't need to worry about
    // what's in there. We will do that later.
    Assert.assertTrue(mds.getStreams(account).size() > 0);
  }

  /**
   * Adds a stream "id2" and deletes it.
   * @throws Exception
   */
  @Test
  public void testDeleteStream() throws Exception {
    int count = mds.getStreams(account).size();

    com.continuuity.metadata.stubs.Stream
      stream = new com.continuuity.metadata.stubs.Stream("id2");
    stream.setName("Serious stream");
    stream.setDescription("Serious stream. Shutup");
    Assert.assertTrue(mds.createStream(account, stream));

    int afterAddCount = mds.getStreams(account).size();
    // Delete the stream now.
    Assert.assertTrue(mds.deleteStream(account, stream));
    int afterDeleteCount = mds.getStreams(account).size();
    Assert.assertTrue(count == afterAddCount-1);
    Assert.assertTrue((afterAddCount - 1) == afterDeleteCount);
  }

  /**
   * Tests listing of streams for a given account.
   * @throws Exception
   */
  @Test
  public void testListStream() throws Exception {
    int before = mds.getStreams(account).size();
    com.continuuity.metadata.stubs.Stream
      stream = new com.continuuity.metadata.stubs.Stream("id3");
    stream.setName("Serious stream");
    stream.setDescription("Serious stream. Shutup");
    Assert.assertTrue(mds.createStream(account, stream));
    Collection<com.continuuity.metadata.stubs.Stream> streams
      = mds.getStreams(account);
    int after = streams.size();
    Assert.assertTrue(after == before + 1);
    for(com.continuuity.metadata.stubs.Stream s : streams) {
      if(s.getId().equals("id3")) {
        Assert.assertTrue("Serious stream".equals(s.getName()));
        Assert.assertTrue("Serious stream. Shutup".equals(s.getDescription()));
      }
    }
    Account account1 = new Account("abc");
    Assert.assertTrue(mds.getStreams(account1).size() == 0);
  }


}
