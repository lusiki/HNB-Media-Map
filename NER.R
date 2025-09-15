# Load all necessary libraries

library(udpipe)
library(dplyr)
library(tibble)
library(progressr)
library(cli)

# Load your data and create a sample
# The code below will create a small sample data frame for demonstration
# Replace with your own code to load the dta data frame from the rds file


dta <- readRDS("D:/LUKA/HNB/HNB_Media_Map/hnb.rds") %>%
  mutate(doc_id = row_number())

output_dir = "D:/Luka/HNB/Language model sample"

dta_sample <- dta %>%
  mutate(doc_id = row_number()) %>%
  filter(nchar(FULL_TEXT) > 10) %>%
  slice_sample(n = 1000)

model_path <- udpipe_download_model(language = "croatian-set")
ud_model_hr <- udpipe_load_model(file = "croatian-set-ud-2.5-191206.udpipe")

# Set up your parameters
batch_size_to_use <- 100
cores_to_use <- parallel::detectCores() - 1


udpipe_annotate_in_batches_and_save <- function(
    data,
    text_col,
    doc_id_col,
    model,
    batch_size = 500,
    cores = 1,
    output_dir,
    resume = FALSE
) {

  # Set the progress handler to display a detailed cli progress bar
  handlers(handler_cli())

  # Start the progress reporting
  with_progress({

    # Create an output directory if it doesn't exist
    if (!dir.exists(output_dir)) {
      dir.create(output_dir)
    }

    # Get the total number of documents and calculate number of batches
    total_docs <- nrow(data)
    num_batches <- ceiling(total_docs / batch_size)

    # Create a progressor with the total number of batches
    p <- progressor(steps = num_batches)

    # Check for existing files if resuming
    if (resume) {
      existing_files <- list.files(output_dir, pattern = "\\.rds$", full.names = TRUE)
      if (length(existing_files) > 0) {
        completed_batches <- length(existing_files)
        # Advance the progress bar to the correct point when resuming
        p(amount = completed_batches, message = sprintf("Resuming... Skipping first %d batches.", completed_batches))
      } else {
        completed_batches <- 0
      }
    } else {
      completed_batches <- 0
    }

    # Process data in batches
    for (i in (completed_batches + 1):num_batches) {
      # Start timing for the current batch
      start_time <- Sys.time()

      # Determine the start and end indices for the current batch
      start_index <- (i - 1) * batch_size + 1
      end_index <- min(i * batch_size, total_docs)

      # Extract the current batch
      batch_data <- data[start_index:end_index, ]

      # Annotate the current batch
      result <- udpipe_annotate(
        model,
        x = batch_data[[text_col]],
        doc_id = batch_data[[doc_id_col]],
        parallel.cores = cores
      )

      # Save the current batch result to a file
      saveRDS(as_tibble(result), file = file.path(output_dir, paste0("batch_", i, ".rds")))

      # Calculate time for the current batch
      batch_time <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 2)

      # Signal one step of progress, including detailed batch info
      p(message = paste0("Processing batch ", i, " of ", num_batches, ". Time: ", batch_time, "s"))
    }
  })

  cat("\nProcessing complete. Combining all files...\n")

  # Combine all results from the batches into a single tibble
  all_files <- list.files(output_dir, pattern = "\\.rds$", full.names = TRUE)
  final_df <- bind_rows(lapply(all_files, readRDS))

  return(final_df)
}



final_annotated_data <- udpipe_annotate_in_batches_and_save(
  data = dta,
  text_col = "FULL_TEXT",
  doc_id_col = "doc_id",
  model = ud_model_hr,
  batch_size = 500,
  cores = parallel::detectCores() - 1,
  output_dir = "D:/Luka/HNB/Language model sample",
  resume = FALSE
)
















