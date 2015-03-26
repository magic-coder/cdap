/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * {@link TwillApplication} to run {@link MapReduceTwillRunnable}
 */
public final class MapReduceTwillApplication implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceTwillApplication.class);
  private final MapReduceSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public MapReduceTwillApplication(Program program, MapReduceSpecification spec,
                                   File hConfig, File cConfig, EventHandler eventHandler) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }


  /**
   * Trace the dependencies files of the given className, using the classLoader,
   * and excluding any class contained in the bootstrapClassPaths and Kryo classes.
   * We need to remove Kryo dependency in the Explore container. Spark introduced version 2.21 version of Kryo,
   * which would be normally shipped to the Explore container. Yet, Hive requires Kryo 2.22,
   * and gets it from the Hive jars - hive-exec.jar to be precise.
   *
   * Nothing is returned if the classLoader does not contain the className.
   */
  public static Set<File> traceDependencies(String className, final Set<String> bootstrapClassPaths,
                                            ClassLoader classLoader)
    throws IOException {
    ClassLoader usingCL = classLoader;
    if (usingCL == null) {
      usingCL = AppFabricServiceRuntimeModule.class.getClassLoader();
    }
    final Set<File> jarFiles = Sets.newHashSet();

    Dependencies.findClassDependencies(
      usingCL,
      new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
            return false;
          }

          if (className.startsWith("com.esotericsoftware.kryo")) {
            return false;
          }

          jarFiles.add(new File(classPathUrl.getFile()));
          return true;
        }
      },
      className
    );

    return jarFiles;
  }

  /**
   * Return the list of absolute paths of the bootstrap classes.
   */
  public static Set<String> getBoostrapClasses() {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      builder.add(file.getAbsolutePath());
      try {
        builder.add(file.getCanonicalPath());
      } catch (IOException e) {
        LOG.warn("Could not add canonical path to aux class path for file {}", file.toString(), e);
      }
    }
    return builder.build();
  }

  @Override
  public TwillSpecification configure() {
    // These resources are for the container that runs the mapred client that will launch the actual mapred job.
    // It does not need much memory.  Memory for mappers and reduces are specified in the MapReduceSpecification,
    // which is configurable by the author of the job.
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();


    TwillSpecification.Builder.MoreFile twillSpecs = TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             ProgramType.MAPREDUCE.name().toLowerCase(),
                             program.getNamespaceId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
        .add(spec.getName(),
             new MapReduceTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
             resourceSpec)
        .withLocalFiles()
          .add(programLocation.getName(), programLocation.toURI())
          .add("hConf.xml", hConfig.toURI())
          .add("cConf.xml", cConfig.toURI());


    try {
      for (File jarFile : traceDependencies("org.apache.hadoop.mapreduce.v2.app.MRClientSecurityInfo",
                                            getBoostrapClasses(), null)) {
        twillSpecs = twillSpecs.add(jarFile.getName(), jarFile);
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to trace Explore dependencies", e);
    }
    return twillSpecs.apply().anyOrder().withEventHandler(eventHandler).build();
  }
}
