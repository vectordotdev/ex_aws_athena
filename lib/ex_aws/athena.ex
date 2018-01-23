defmodule ExAws.Athena do
  @moduledoc """
  Operations on AWS Athena

  http://docs.aws.amazon.com/athena/latest/APIReference/API_Operations.html
  """

  import ExAws.Utils, only: [camelize_keys: 1]

  require Logger

  @type query_execution_id :: String.t
  @type get_query_execution_opts :: []

  @namespace "AmazonAthena"

  @spec get_query_execution(query_execution_id) :: ExAws.Operation.JSON.t
  def get_query_execution(query_execution_id) do
    data = %{"QueryExecutionId" => query_execution_id}
    request(:get_query_execution, data)
  end

  @doc """
  Retreives the results of a query if the query has completed. Can be streamed.
  """
  @spec get_query_results(query_execution_id, get_query_execution_opts) :: ExAws.Operation.JSON.t
  def get_query_results(query_execution_id, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.put("QueryExecutionId", query_execution_id)

    request(
      :get_query_results,
      data,
      %{
        stream_builder: &ExAws.Athena.Lazy.stream_query_results!(query_execution_id, opts, &1)
      }
    )
  end

  def start_query_execution(query_string, result_output_location, opts \\ []) do
    client_request_token = Keyword.get_lazy(opts, :client_request_token, fn -> random_string(64) end)

    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "ClientRequestToken" => client_request_token,
        "QueryString" => query_string,
        "ResultConfiguration" => %{
          "OutputLocation" => result_output_location
        }
      })

    request(:start_query_execution, data)
  end

  @spec stop_query_execution(query_execution_id) :: ExAws.Operation.JSON.t
  def stop_query_execution(query_execution_id) do
    data = %{"QueryExecutionId" => query_execution_id}
    request(:stop_query_execution, data)
  end

  defp normalize_opts(opts) do
    opts
    |> Map.new()
    |> camelize_keys()
    |> Enum.map(fn {key, value} ->
      if Keyword.keyword?(value) do
        normalized_value = normalize_opts(value)
        {key, normalized_value}
      else
        {key, value}
      end
    end)
    |> Enum.into(%{})
  end

  defp request(action, data, opts \\ %{}) do
    operation =
      action
      |> Atom.to_string()
      |> Macro.camelize()

    ExAws.Operation.JSON.new(:athena, %{
      data: data,
      headers: [
        {"x-amz-target", "#{@namespace}.#{operation}"},
        {"content-type", "application/x-amz-json-1.1"}
      ]
    } |> Map.merge(opts))
  end

  defp random_string(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.encode64()
    |> binary_part(0, length)
  end
end
