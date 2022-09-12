#include <jvmti.h>
#include <string>
#include <iostream>

extern "C" void JNICALL JVM_StartThread(JNIEnv* env, jthread thread);

std::string executePath;

void JNICALL ThreadStart0Hook(JNIEnv* env, jthread thread) {

	JavaVM* vm;
	env->GetJavaVM(&vm);

	jvmtiEnv* jvmti;
	vm->GetEnv((void**)&jvmti, JVMTI_VERSION_1_0);

	jvmtiFrameInfo frames[1000];
	jint count;
	jvmtiError err;
	jthread currentThread;
	jvmti->GetCurrentThread(&currentThread);

	err = jvmti->GetStackTrace(currentThread, 0, 1000, frames, &count);
	if(err == JVMTI_ERROR_NONE && count >= 1){
		char *mname;
		jclass declaring_class;
		char *declaringClassName;

		for(int i=0; i< count; i++){
			err = jvmti->GetMethodName(frames[i].method, &mname, NULL, NULL);
            //printf(">>>>> method name: %s signature: %s generic: %s\n", mname, msignature, mgeneric);
			err = jvmti->GetMethodDeclaringClass(frames[i].method, &declaring_class);
			err = jvmti->GetClassSignature(declaring_class, &declaringClassName, NULL);
			if(err == JVMTI_ERROR_NONE) {
				 //printf(">>> %s %s\n", declaringClassName, mname);
				 std::string callPath;
				 callPath.append(declaringClassName);
				 callPath.append(mname);
				if (callPath.find(executePath) != std::string::npos) {
				    //std::cout << "Cannot create threads! " << callPath << "  |  " << executePath << std::endl;
					env->ThrowNew(env->FindClass("java/lang/Error"), "Filters and Functions cannot create Threads!");
				}
			}
		}
	}
    JVM_StartThread(env, thread);
}

void JNICALL VMInit(jvmtiEnv* jvmti, JNIEnv* env, jthread thread) {
    jclass threadClass = env->FindClass("java/lang/Thread");
    JNINativeMethod start0Method = {(char*)"start0", (char*)"()V", (void*)ThreadStart0Hook};
    env->RegisterNatives(threadClass, &start0Method, 1);
}

JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM* vm, char* options, void* reserved) {
    executePath.append(options);
    //std::cout << "executePath: " << executePath << "\n";

    jvmtiEnv* jvmti;
    vm->GetEnv((void**)&jvmti, JVMTI_VERSION_1_0);

    jvmtiEventCallbacks callbacks = {0};
    callbacks.VMInit = VMInit;
    jvmti->SetEventCallbacks(&callbacks, sizeof(callbacks));
    jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_INIT, NULL);

    return 0;
}