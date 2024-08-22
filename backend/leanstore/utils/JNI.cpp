#include "JNI.hpp"

#include "Exceptions.hpp"
#include "leanstore/Config.hpp"

#include "jni.h"

#include <atomic>
#include <cstring>
#include <filesystem>
#include <functional>
#include <stdexcept>

namespace java
{
namespace lang
{
static jclass jc_Throwable;
static jmethodID jm_Throwable_getMessage;
}  // namespace lang
}  // namespace java

namespace bookkeeper
{
static jclass jc_ClientConfiguration;
static jmethodID jm_ClientConfiguration_init;
static jmethodID jm_ClientConfiguration_setMetadataServiceUri;

static jclass jc_DigestType;
static jfieldID jf_DigestType_CRC32;
static jfieldID jf_DigestType_CRC32C;
static jfieldID jf_DigestType_DUMMY;
static jfieldID jf_DigestType_MAC;

static jclass jc_BookKeeper;
static jmethodID jm_BookKeeper_init;
static jmethodID jm_BookKeeper_createLedger;
static jmethodID jm_BookKeeper_close;

static jclass jc_AsyncLedgerContext;
static jmethodID jm_AsyncLedgerContext_init;
static jmethodID jm_AsyncLedgerContext_appendAsync;
static jmethodID jm_AsyncLedgerContext_awaitAll;
static jmethodID jm_AsyncLedgerContext_close;
}  // namespace bookkeeper

namespace jni
{
static const jint VERSION = JNI_VERSION_10;

class JVMRef
{
  private:
   JavaVM* jvm;

  public:
   JVMRef(JavaVM* jvm) : jvm(jvm) {}
   JVMRef(const JVMRef&) = delete;
   JVMRef(JVMRef&& ref) : jvm(std::move(ref.jvm)) {};
   ~JVMRef() { jvm->DestroyJavaVM(); }

   JVMRef& operator=(const JVMRef&) = delete;
   JVMRef& operator=(JVMRef&& ref)
   {
      jvm = std::move(ref.jvm);
      return *this;
   }

   JNIEnv* getEnv()
   {
      JNIEnv* env;
      jvm->GetEnv(reinterpret_cast<void**>(&env), VERSION);
      return env;
   }

   void attachThread()
   {
      JNIEnv* env;
      if (jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != JNI_OK) {
         SetupFailed("Could not attach the current thread to the JVM");
      }
   }

   void detachThread()
   {
      if (jvm->DetachCurrentThread() != JNI_OK) {
         SetupFailed("Could not detach the current thread from the JVM");
      }
   }
};

static std::atomic_bool IS_LOADED;
static std::unique_ptr<JVMRef> JVM_REF;

static jclass mustFindClass(JNIEnv* env, const std::string& name)
{
   jclass clazz = env->FindClass(name.c_str());
   if (clazz == nullptr) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      SetupFailed("Could not find class");
   }
   return clazz;
}

static jfieldID mustGetStaticFieldID(JNIEnv* env, jclass clazz, const std::string& name, const std::string& signature)
{
   jfieldID id = env->GetStaticFieldID(clazz, name.c_str(), signature.c_str());
   if (id == nullptr) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      SetupFailed("Could not find static field");
   }
   return id;
}

static jmethodID mustGetMethodID(JNIEnv* env, jclass clazz, const std::string& name, const std::string& signature)
{
   jmethodID id = env->GetMethodID(clazz, name.c_str(), signature.c_str());
   if (id == nullptr) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      SetupFailed("Could not find method");
   }
   return id;
}

static void check_java_exception(JNIEnv* env)
{
   jthrowable throwable_ptr = env->ExceptionOccurred();
   if (throwable_ptr != nullptr) {
      env->ExceptionClear();
      java::lang::LocalThrowable throwable((jni::LocalRef(throwable_ptr)));
      std::runtime_error error(throwable.getMessage());
      throw error;
   }
}

static void loadClassAndMethodReferences(JNIEnv* env)
{
   // Load and cache java.lang.Throwable.
   {
      jclass clazz = mustFindClass(env, "java/lang/Throwable");
      java::lang::jc_Throwable = reinterpret_cast<jclass>(env->NewGlobalRef(clazz));
      java::lang::jm_Throwable_getMessage = mustGetMethodID(env, java::lang::jc_Throwable, "getMessage", "()Ljava/lang/String;");
      env->DeleteLocalRef(clazz);
   }
   // Load and cache org.apache.bookkeeper.conf.ClientConfiguration.
   {
      jclass clazz = mustFindClass(env, "org/apache/bookkeeper/conf/ClientConfiguration");
      bookkeeper::jc_ClientConfiguration = reinterpret_cast<jclass>(env->NewGlobalRef(clazz));
      bookkeeper::jm_ClientConfiguration_init = mustGetMethodID(env, bookkeeper::jc_ClientConfiguration, "<init>", "()V");
      bookkeeper::jm_ClientConfiguration_setMetadataServiceUri = mustGetMethodID(
          env, bookkeeper::jc_ClientConfiguration, "setMetadataServiceUri", "(Ljava/lang/String;)Lorg/apache/bookkeeper/conf/AbstractConfiguration;");
      env->DeleteLocalRef(clazz);
   }
   // Load and cache org.apache.bookkeeper.client.BookKeeper.DigestType.
   {
      jclass clazz = mustFindClass(env, "org/apache/bookkeeper/client/BookKeeper$DigestType");
      bookkeeper::jc_DigestType = reinterpret_cast<jclass>(env->NewGlobalRef(clazz));
      bookkeeper::jf_DigestType_CRC32 =
          mustGetStaticFieldID(env, bookkeeper::jc_DigestType, "CRC32", "Lorg/apache/bookkeeper/client/BookKeeper$DigestType;");
      bookkeeper::jf_DigestType_CRC32C =
          mustGetStaticFieldID(env, bookkeeper::jc_DigestType, "CRC32C", "Lorg/apache/bookkeeper/client/BookKeeper$DigestType;");
      bookkeeper::jf_DigestType_DUMMY =
          mustGetStaticFieldID(env, bookkeeper::jc_DigestType, "DUMMY", "Lorg/apache/bookkeeper/client/BookKeeper$DigestType;");
      bookkeeper::jf_DigestType_MAC =
          mustGetStaticFieldID(env, bookkeeper::jc_DigestType, "MAC", "Lorg/apache/bookkeeper/client/BookKeeper$DigestType;");
      env->DeleteLocalRef(clazz);
   }
   // Load and cache org.apache.bookkeeper.client.BookKeeper.
   {
      jclass clazz = mustFindClass(env, "org/apache/bookkeeper/client/BookKeeper");
      bookkeeper::jc_BookKeeper = reinterpret_cast<jclass>(env->NewGlobalRef(clazz));
      bookkeeper::jm_BookKeeper_init =
          mustGetMethodID(env, bookkeeper::jc_BookKeeper, "<init>", "(Lorg/apache/bookkeeper/conf/ClientConfiguration;)V");
      bookkeeper::jm_BookKeeper_createLedger =
          mustGetMethodID(env, bookkeeper::jc_BookKeeper, "createLedger",
                          "(IILorg/apache/bookkeeper/client/BookKeeper$DigestType;[B)Lorg/apache/bookkeeper/client/LedgerHandle;");
      bookkeeper::jm_BookKeeper_close = mustGetMethodID(env, bookkeeper::jc_BookKeeper, "close", "()V");
      env->DeleteLocalRef(clazz);
   }
   // Load and cache de.tu_darmstadt.systems.AsyncLedgerContext
   {
      jclass clazz = mustFindClass(env, "de/tu_darmstadt/systems/AsyncLedgerContext");
      bookkeeper::jc_AsyncLedgerContext = reinterpret_cast<jclass>(env->NewGlobalRef(clazz));
      bookkeeper::jm_AsyncLedgerContext_init =
          mustGetMethodID(env, bookkeeper::jc_AsyncLedgerContext, "<init>", "(Lorg/apache/bookkeeper/client/LedgerHandle;)V");
      bookkeeper::jm_AsyncLedgerContext_appendAsync = mustGetMethodID(env, bookkeeper::jc_AsyncLedgerContext, "appendAsync", "([B)V");
      bookkeeper::jm_AsyncLedgerContext_awaitAll = mustGetMethodID(env, bookkeeper::jc_AsyncLedgerContext, "awaitAll", "()[J");
      bookkeeper::jm_AsyncLedgerContext_close = mustGetMethodID(env, bookkeeper::jc_AsyncLedgerContext, "close", "()V");
      env->DeleteLocalRef(clazz);
   }
}

static void unloadClassAndMethodReferences(JNIEnv* env)
{
   env->DeleteGlobalRef(java::lang::jc_Throwable);
   env->DeleteGlobalRef(bookkeeper::jc_ClientConfiguration);
   env->DeleteGlobalRef(bookkeeper::jc_DigestType);
   env->DeleteGlobalRef(bookkeeper::jc_BookKeeper);
   env->DeleteGlobalRef(bookkeeper::jc_AsyncLedgerContext);
}

jobject getJObject(JObjectRef& ref)
{
   return ref.object;
}
}  // namespace jni

std::string collectJarPaths()
{
   char delim = ':';
   std::string directories = FLAGS_bookkeeper_jar_directories;
   std::string jar_paths = "";
   std::size_t current, previous = 0;
   std::function<void(std::filesystem::path&)> collect = [&](std::filesystem::path& directory) {
      for (const auto& entry : std::filesystem::directory_iterator(directory)) {
         if (entry.is_regular_file() && entry.path().extension() == ".jar") {
            if (!jar_paths.empty()) {
               jar_paths.push_back(delim);
            }
            jar_paths.append(entry.path().string());
         }
      }
   };
   try {
      while ((current = directories.find(delim, previous)) != std::string::npos) {
         std::filesystem::path directory(directories.substr(previous, current - previous));
         collect(directory);
         previous = current + 1;
      }
      std::filesystem::path directory(directories.substr(previous, current - previous));
      collect(directory);
   } catch (const std::filesystem::filesystem_error& e) {
      std::cerr << "Filesystem error: " << e.what() << std::endl;
   } catch (const std::exception& e) {
      std::cerr << "General error: " << e.what() << std::endl;
   }
   return jar_paths;
}

void jni::init(std::string& classpath)
{
   bool expect_is_loaded = false;
   if (IS_LOADED.compare_exchange_strong(expect_is_loaded, true)) {
      JavaVMOption options[1];
      options[0].optionString = classpath.data();
      JavaVMInitArgs init_args = {
          .version = JNI_VERSION_10,
          .nOptions = 1,
          .options = options,
          .ignoreUnrecognized = false,
      };
      JavaVM* jvm;
      JNIEnv* env;
      if (JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), reinterpret_cast<void*>(&init_args)) != JNI_OK) {
         SetupFailed("Could not create Java VM");
      }
      loadClassAndMethodReferences(env);
      JVM_REF = std::make_unique<JVMRef>(jvm);
   }
}

void jni::deinit()
{
   bool expect_is_loaded = true;
   if (IS_LOADED.compare_exchange_strong(expect_is_loaded, false)) {
      unloadClassAndMethodReferences(JVM_REF->getEnv());
      delete JVM_REF.release();
   }
}

void jni::attachThread()
{
   JVM_REF->attachThread();
}

void jni::detachThread()
{
   JVM_REF->attachThread();
}

jni::JObjectRef::JObjectRef(jobject object) : object(object) {}

template <typename... Args>
jobject jni::JObjectRef::callNonVirtualObjectMethod(jclass clazz, jmethodID methodID, Args... args)
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   return env->CallNonvirtualObjectMethod(object, clazz, methodID, args...);
}

jni::LocalRef::LocalRef(jobject object) : JObjectRef(object) {}
jni::LocalRef::LocalRef(LocalRef&& ref) : JObjectRef(std::move(ref.object))
{
   ref.object = nullptr;
};
jni::LocalRef::~LocalRef()
{
   if (object != nullptr) {
      JVM_REF->getEnv()->DeleteLocalRef(object);
   }
};

jni::LocalRef& jni::LocalRef::operator=(LocalRef&& ref)
{
   object = std::move(ref.object);
   ref.object = nullptr;
   return *this;
}

jni::GlobalRef::GlobalRef(jni::LocalRef ref) : JObjectRef(JVM_REF->getEnv()->NewGlobalRef(ref.object)) {}
jni::GlobalRef::GlobalRef(const GlobalRef& ref) : jni::JObjectRef(JVM_REF->getEnv()->NewGlobalRef(ref.object)) {}
jni::GlobalRef::GlobalRef(GlobalRef&& ref) : JObjectRef(std::move(ref.object))
{
   ref.object = nullptr;
};
jni::GlobalRef::~GlobalRef()
{
   if (object != nullptr) {
      JVM_REF->getEnv()->DeleteGlobalRef(object);
   }
};

jni::GlobalRef& jni::GlobalRef::operator=(const jni::GlobalRef& ref)
{
   object = JVM_REF->getEnv()->NewGlobalRef(ref.object);
   return *this;
}

jni::GlobalRef& jni::GlobalRef::operator=(GlobalRef&& ref)
{
   object = std::move(ref.object);
   ref.object = nullptr;
   return *this;
}

java::lang::LocalThrowable::LocalThrowable(jni::LocalRef ref) : ref(std::move(ref)) {}

std::string java::lang::LocalThrowable::getMessage()
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   jstring message = reinterpret_cast<jstring>(ref.callNonVirtualObjectMethod(java::lang::jc_Throwable, java::lang::jm_Throwable_getMessage));
   const char* message_data = env->GetStringUTFChars(message, nullptr);
   std::string message_str(message_data, env->GetStringUTFLength(message));
   env->ReleaseStringUTFChars(message, message_data);
   env->DeleteLocalRef(message);
   return message_str;
}

bookkeeper::LocalClientConfiguration::LocalClientConfiguration()
    : ref(jni::LocalRef(jni::JVM_REF->getEnv()->NewObject(bookkeeper::jc_ClientConfiguration, bookkeeper::jm_ClientConfiguration_init)))
{
}

bookkeeper::LocalClientConfiguration& bookkeeper::LocalClientConfiguration::setMetadataServiceUri(std::string& uri)
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   jstring uri_jstring = env->NewStringUTF(uri.c_str());
   ref.callNonVirtualObjectMethod(bookkeeper::jc_ClientConfiguration, bookkeeper::jm_ClientConfiguration_setMetadataServiceUri, uri_jstring);
   env->DeleteLocalRef(uri_jstring);
   return *this;
}

bookkeeper::LocalDigestType::LocalDigestType(jni::LocalRef ref) : ref(std::move(ref)) {}

bookkeeper::LocalDigestType bookkeeper::LocalDigestType::CRC32()
{
   jobject ptr = jni::JVM_REF->getEnv()->GetStaticObjectField(bookkeeper::jc_DigestType, bookkeeper::jf_DigestType_CRC32);
   return bookkeeper::LocalDigestType(jni::LocalRef(ptr));
}

bookkeeper::LocalDigestType bookkeeper::LocalDigestType::CRC32C()
{
   jobject ptr = jni::JVM_REF->getEnv()->GetStaticObjectField(bookkeeper::jc_DigestType, bookkeeper::jf_DigestType_CRC32C);
   return bookkeeper::LocalDigestType(jni::LocalRef(ptr));
}

bookkeeper::LocalDigestType bookkeeper::LocalDigestType::DUMMY()
{
   jobject ptr = jni::JVM_REF->getEnv()->GetStaticObjectField(bookkeeper::jc_DigestType, bookkeeper::jf_DigestType_DUMMY);
   return bookkeeper::LocalDigestType(jni::LocalRef(ptr));
}

bookkeeper::LocalDigestType bookkeeper::LocalDigestType::MAC()
{
   jobject ptr = jni::JVM_REF->getEnv()->GetStaticObjectField(bookkeeper::jc_DigestType, bookkeeper::jf_DigestType_MAC);
   return bookkeeper::LocalDigestType(jni::LocalRef(ptr));
}

bookkeeper::GlobalBookKeeper::GlobalBookKeeper(bookkeeper::LocalClientConfiguration configuration)
    : ref(jni::GlobalRef(jni::LocalRef(
          jni::JVM_REF->getEnv()->NewObject(bookkeeper::jc_BookKeeper, bookkeeper::jm_BookKeeper_init, jni::getJObject(configuration.ref)))))
{
   jni::check_java_exception(jni::JVM_REF->getEnv());
}

bookkeeper::GlobalBookKeeper::~GlobalBookKeeper()
{
   ref.callNonVirtualObjectMethod(bookkeeper::jc_BookKeeper, bookkeeper::jm_BookKeeper_close);
   jni::check_java_exception(jni::JVM_REF->getEnv());
}

jni::LocalRef bookkeeper::GlobalBookKeeper::createLedger(int ensemble, int quorum, LocalDigestType digest, char* password, int password_length)
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   jbyteArray password_jbyte_array = env->NewByteArray(password_length);
   jbyte* password_bytes = env->GetByteArrayElements(password_jbyte_array, nullptr);
   memcpy(password_bytes, password, password_length);
   env->ReleaseByteArrayElements(password_jbyte_array, password_bytes, 0);
   jobject ledger = ref.callNonVirtualObjectMethod(bookkeeper::jc_BookKeeper, bookkeeper::jm_BookKeeper_createLedger, ensemble, quorum,
                                                   jni::getJObject(digest.ref), password_jbyte_array);
   env->DeleteLocalRef(password_jbyte_array);
   jni::check_java_exception(env);
   return jni::LocalRef(ledger);
}

bookkeeper::GlobalAsyncLedgerContext::GlobalAsyncLedgerContext(jni::LocalRef ledger_ref)
    : ref(jni::GlobalRef(jni::LocalRef(
          jni::JVM_REF->getEnv()->NewObject(bookkeeper::jc_AsyncLedgerContext, bookkeeper::jm_AsyncLedgerContext_init, jni::getJObject(ledger_ref)))))
{
}

bookkeeper::GlobalAsyncLedgerContext::~GlobalAsyncLedgerContext()
{
   ref.callNonVirtualObjectMethod(bookkeeper::jc_AsyncLedgerContext, bookkeeper::jm_AsyncLedgerContext_close);
   jni::check_java_exception(jni::JVM_REF->getEnv());
}

void bookkeeper::GlobalAsyncLedgerContext::appendAsync(unsigned char* payload, int payload_length)
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   jbyteArray payload_jbyte_array = env->NewByteArray(payload_length);
   jbyte* payload_bytes = env->GetByteArrayElements(payload_jbyte_array, nullptr);
   memcpy(payload_bytes, payload, payload_length);
   env->ReleaseByteArrayElements(payload_jbyte_array, payload_bytes, 0);
   ref.callNonVirtualObjectMethod(bookkeeper::jc_AsyncLedgerContext, bookkeeper::jm_AsyncLedgerContext_appendAsync, payload_jbyte_array);
   env->DeleteLocalRef(payload_jbyte_array);
   jni::check_java_exception(jni::JVM_REF->getEnv());
}

std::vector<long> bookkeeper::GlobalAsyncLedgerContext::awaitAll()
{
   JNIEnv* env = jni::JVM_REF->getEnv();
   jlongArray entry_ids_jlong_array =
       reinterpret_cast<jlongArray>(ref.callNonVirtualObjectMethod(bookkeeper::jc_AsyncLedgerContext, bookkeeper::jm_AsyncLedgerContext_awaitAll));
   jlong* entry_ids_jlong = env->GetLongArrayElements(entry_ids_jlong_array, nullptr);
   jlong entry_ids_length = env->GetArrayLength(entry_ids_jlong_array);
   std::vector<long> entry_ids(entry_ids_length);
   mempcpy(entry_ids.data(), entry_ids_jlong, entry_ids_length * sizeof(long));
   env->ReleaseLongArrayElements(entry_ids_jlong_array, entry_ids_jlong, JNI_ABORT);
   env->DeleteLocalRef(entry_ids_jlong_array);
   jni::check_java_exception(jni::JVM_REF->getEnv());
   return entry_ids;
}
