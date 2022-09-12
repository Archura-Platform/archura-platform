# Agent 

We need an agent to prevent filters and functions to start Threads and create connections. 
 
## JDK Build

We need `jvmti.h` header to compile the agent, therefore we need to compile the JDK from scratch.

* clone the `https://github.com/openjdk/jdk` repository
  * `git clone https://github.com/openjdk/jdk.git`
* checkout the version 19 branch, i.e. `jdk-19+36`
  * `git checkout jdk-19+36`
* you need java version 18 or 19 to build the JDK
  * `sdk install java 19.ea.36-open`
* you need a newer version of the gcc that supports the cpp version used by JDK, if you don't install it and use it to build the JDK
  * `sudo yum -y update`
  * `sudo yum install -y centos-release-scl devtoolset-8-gcc devtoolset-8-gcc-c++`
  * `scl enable devtoolset-8 -- bash` 
* follow the README for build instructions, you might need to install update some dependencies, 
  * `sudo yum install -y alsa-lib-devel cups-devel libXtst-devel libXt-devel libXrender-devel libXrandr-devel libXi-devel`
  * `bash configure`
  * `make images`
* The following might fail towards the end, it's still ok
  * `make run-test-tier1` 
* The necessary `jvmti.h` file should be under `include` folder
  * `find . -iname "jvmti*.h*"`
  * `cd ./build/linux-x86_64-server-release/images/jdk/include/`
* copy the `filtersfunctions.cpp` file into this folder, after that you can build the agent, you'll need the files from `jdk/include` and `native/include` folders
  * `g++ -I/home/USERNAME/jdk/openjdk-git/build/linux-x86_64-server-release/images/jdk/include -I/home/USERNAME/jdk/openjdk-git/src/java.base/unix/native/include -fPIC -shared -olibfiltersfunctions.so -Wl,-soname,libfiltersfunctions.so filtersfunctions.cpp`
* now you can run the java application with the agent and pass the entry 'Class/Method' for the filters and functions 
  * `java -agentpath:/home/USERNAME/jdk/openjdk-git/build/linux-x86_64-server-release/images/jdk/include/libfiltersfunctions.so='Lio/archura/platform/internal/RequestHandler;execute' --enable-preview --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED  -jar /tmp/archura-platform-0.0.1-SNAPSHOT.jar`
* you should see a warning message similar to the following, indicating that the `Thread.start0` native method is replaced
  * `[0.041s][warning][jni,resolve] Re-registering of platform native method: java.lang.Thread.start0()V from code in a different classloader`
* and when you try to create a Thread within a Filter or a Function, you should see an error similar to the following
  * `java.lang.Error: Filters and Functions cannot create Threads!`
