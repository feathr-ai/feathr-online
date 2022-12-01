#!/bin/sh

cd $(dirname $0)

targets=(x86_64-pc-windows-gnu x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-apple-darwin aarch64-apple-darwin)
# targets=(x86_64-apple-darwin aarch64-apple-darwin)

for i in "${targets[@]}"
do
	cross build --target $i --package feathr_piper_jni --release
done

cp target/x86_64-pc-windows-gnu/release/feathr_piper_jni.dll java/feathrpiper/src/main/resources/native/feathr_piper_jni_windows_x86_64.dll
cp target/aarch64-unknown-linux-gnu/release/libfeathr_piper_jni.so java/feathrpiper/src/main/resources/native/libfeathr_piper_jni_linux_aarch64.so
cp target/x86_64-unknown-linux-gnu/release/libfeathr_piper_jni.so java/feathrpiper/src/main/resources/native/libfeathr_piper_jni_linux_x86_64.so
cp target/x86_64-apple-darwin/release/libfeathr_piper_jni.dylib java/feathrpiper/src/main/resources/native/libfeathr_piper_jni_osx_x86_64.dylib 
cp target/aarch64-apple-darwin/release/libfeathr_piper_jni.dylib java/feathrpiper/src/main/resources/native/libfeathr_piper_jni_osx_aarch64.dylib

cd java
./gradlew jar
