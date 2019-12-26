/*
   Copyright 2018 Callide Technologies LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  */

package xyz.callide.mesa.data

trait DataSetUtil {

  protected val data3: DataSet = DataSet.fromCsvResource("data3.csv")

  protected val data4: DataSet = DataSet.fromCsvResource("data4.csv")

  protected val data5: DataSet = DataSet.fromCsvResource("data5.csv")
}
