defmodule ExAws.Athena.Lazy do
  @moduledoc false

  def stream_query_results!(query_execution_id, opts, config) do
    request_fun = fn fun_opts ->
      ExAws.Athena.get_query_results(query_execution_id, Keyword.merge(opts, fun_opts))
      |> ExAws.request!(config)
    end

    build_request_stream(request_fun)
  end

  defp build_request_stream(request_fun) do
    Stream.resource(
      fn -> {request_fun, []} end,
      fn
        :quit ->
          {:halt, nil}

        {fun, args} ->
          case fun.(args) do
            %{"ResultSet" => %{"Rows" => rows}, "NextToken" => next_token} ->
              {rows, {fun, [next_token: next_token]}}

            %{"ResultSet" => %{"Rows" => rows}} ->
              {rows, :quit}
          end
      end,
      &pass/1
    )
  end

  defp pass(val), do: val
end
