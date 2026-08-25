/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark;

import org.lance.spark.utils.Optional;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Objects;

public class LanceRef implements Serializable {
  private static final long serialVersionUID = 7967911672598737764L;

  private final Optional<Long> versionNumber;
  private final Optional<String> branchName;
  private final Optional<String> tagName;

  public LanceRef(
      Optional<Long> versionNumber, Optional<String> branchName, Optional<String> tagName) {
    this.versionNumber = versionNumber;
    this.branchName = branchName;
    this.tagName = tagName;
  }

  public static LanceRef ofMain(long versionNumber) {
    Preconditions.checkArgument(versionNumber > 0, "versionNumber must be greater than 0");
    return new LanceRef(Optional.of(versionNumber), Optional.empty(), Optional.empty());
  }

  public static LanceRef ofMain() {
    return new LanceRef(Optional.empty(), Optional.empty(), Optional.empty());
  }

  public static LanceRef ofBranch(String branchName) {
    Preconditions.checkArgument(
        branchName != null && !branchName.isEmpty(), "branchName must not be empty");
    return new LanceRef(Optional.empty(), Optional.of(branchName), Optional.empty());
  }

  public static LanceRef ofBranch(String branchName, long versionNumber) {
    Preconditions.checkArgument(
        branchName != null && !branchName.isEmpty(), "branchName must not be empty");
    Preconditions.checkArgument(versionNumber > 0, "versionNumber must be greater than 0");
    return new LanceRef(Optional.of(versionNumber), Optional.of(branchName), Optional.empty());
  }

  public static LanceRef ofTag(String tagName) {
    Preconditions.checkArgument(tagName != null && !tagName.isEmpty(), "tagName must not be empty");
    return new LanceRef(Optional.empty(), Optional.empty(), Optional.of(tagName));
  }

  public Optional<Long> getVersionNumber() {
    return versionNumber;
  }

  public Optional<String> getBranchName() {
    return branchName;
  }

  public Optional<String> getTagName() {
    return tagName;
  }

  public boolean isMain() {
    return branchName.isEmpty() && tagName.isEmpty();
  }

  public boolean isBranch() {
    return branchName.isPresent();
  }

  public boolean isTag() {
    return tagName.isPresent();
  }

  public boolean isBranchOrTag() {
    return isBranch() || isTag();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceRef lanceRef = (LanceRef) o;
    return Objects.equals(versionNumber, lanceRef.versionNumber)
        && Objects.equals(branchName, lanceRef.branchName)
        && Objects.equals(tagName, lanceRef.tagName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionNumber, branchName, tagName);
  }

  @Override
  public String toString() {
    if (isTag()) {
      return "tag " + tagName.get();
    }
    if (isBranch() && versionNumber.isPresent()) {
      return "branch " + branchName.get() + " version " + versionNumber.get();
    }
    if (isBranch()) {
      return "branch " + branchName.get();
    }
    if (versionNumber.isPresent()) {
      return "version " + versionNumber.get();
    }
    return "main";
  }
}
