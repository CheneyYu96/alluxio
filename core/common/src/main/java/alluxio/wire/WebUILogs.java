/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import alluxio.util.webui.UIFileInfo;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI logs information.
 */
@NotThreadSafe
public final class WebUILogs implements Serializable {
  private String mCurrentPath;
  private boolean mDebug;
  private String mFatalError;
  private String mFileData;
  private List<UIFileInfo> mFileInfos;
  private String mInvalidPathError;
  private int mNTotalFile;
  private long mViewingOffset;

  /**
   * Creates a new instance of {@link WebUIWorkers}.
   */
  public WebUILogs() {
  }

  public String getCurrentPath() {
    return mCurrentPath;
  }

  public boolean getDebug() {
    return mDebug;
  }

  public String getFatalError() {
    return mFatalError;
  }

  public String getFileData() {
    return mFileData;
  }

  public List<UIFileInfo> getFileInfos() {
    return mFileInfos;
  }

  public String getInvalidPathError() {
    return mInvalidPathError;
  }

  public int getNTotalFile() {
    return mNTotalFile;
  }

  public long getViewingOffset() {
    return mViewingOffset;
  }

  public WebUILogs setCurrentPath(String currentPath) {
    mCurrentPath = currentPath;
    return this;
  }

  public WebUILogs setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  public WebUILogs setFatalError(String fatalError) {
    mFatalError = fatalError;
    return this;
  }

  public WebUILogs setFileData(String fileData) {
    mFileData = fileData;
    return this;
  }

  public WebUILogs setFileInfos(List<UIFileInfo> fileInfos) {
    mFileInfos = fileInfos;
    return this;
  }

  public WebUILogs setInvalidPathError(String invalidPathError) {
    mInvalidPathError = invalidPathError;
    return this;
  }

  public WebUILogs setNTotalFile(int nTotalFile) {
    mNTotalFile = nTotalFile;
    return this;
  }

  public WebUILogs setViewingOffset(long viewingOffset) {
    mViewingOffset = viewingOffset;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mCurrentPath", mCurrentPath).add("mDebug", mDebug)
        .add("mFatalError", mFatalError).add("mFileData", mFileData).add("mFileInfos", mFileInfos)
        .add("mInvalidPathError", mInvalidPathError).add("mNTotalFile", mNTotalFile)
        .add("mViewingOffset", mViewingOffset).toString();
  }
}