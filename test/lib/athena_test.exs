defmodule ExAws.AthenaTest do
  use ExUnit.Case, async: true

  alias ExAws.Athena

  ## NOTE:
  # These tests are not intended to be operational examples, but intead mere
  # ensure that the form of the data to be sent to AWS is correct.
  #

  test "ExAws.Athena.start_query_execution/2" do
    sql_query = "SELECT * FROM my_table;"
    results_location = "s3://my-bucket/path"
    request = Athena.start_query_execution(sql_query, results_location, query_execution_context: [database: "my-database"])
    assert request.data["QueryString"] == "SELECT * FROM my_table;"
    assert request.data["ResultConfiguration"] == %{"OutputLocation" => "s3://my-bucket/path"}
    assert request.data["ClientRequestToken"]
    assert request.data["QueryExecutionContext"] == %{"Database" => "my-database"}
  end
end
