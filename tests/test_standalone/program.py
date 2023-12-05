import argparse
import os, sys, json
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import mlflow
from torchvision import datasets, transforms
from torch.optim.lr_scheduler import StepLR
from loguru import logger
from mlflow.tracking import MlflowClient



class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout(0.25)
        self.dropout2 = nn.Dropout(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = F.relu(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output


def train(tracking, run_id, model, device, train_loader, optimizer, epoch, log_interval=50):

    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % log_interval == 0:
            logger.info('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), loss.item()))
            
            tracking.log_metric(run_id, 'loss', loss.item())


def test(tracking, run_id, model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)

    logger.info('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset),
        100. * correct / len(test_loader.dataset)))

    tracking.log_metric(run_id, 'loss_test',test_loss)




def main():
    # Training settings
    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser = argparse.ArgumentParser()

    parser.add_argument('-j','--job', action='store',
            dest='job', required = True,
                help = "The job config file.")

    if len(sys.argv)==1:
      parser.print_help()
      sys.exit(1)
    
    args = parser.parse_args()


    logger.info('Starting job...')

    # getting parameters from the job configuration
    job             = json.load(open(args.job, 'r'))
    seed            = job['seed']
    lr              = job['lr']
    batch_size      = job['batch_size']
    test_batch_size = job['test_batch_size']
    gamma           = job['gamma']
    epochs          = job['epochs']


    # getting parameters from the server
    device       = int(os.environ['CUDA_VISIBLE_DEVICES'])
    workarea     = os.environ['JOB_WORKAREA']
    job_id       = os.environ['JOB_ID']
    run_id       = os.environ['MLFLOW_RUN_ID']
    tracking_url = os.environ['MLFLOW_URL']
    dry_run      = os.environ['JOB_DRY_RUN'] == 'true'



    tracking = MlflowClient(tracking_url)
    mlflow.set_tracking_uri(tracking_url)

    cuda_available = torch.cuda.is_available()
    device = -1 if device >= 0 and not torch.cuda.is_available() else device
    epochs = 1 if dry_run else epochs
    torch.manual_seed(seed)

   
    transform=transforms.Compose([transforms.ToTensor(),
                                  transforms.Normalize((0.1307,), (0.3081,))])

    dataset_train = datasets.MNIST(f'{workarea}/data', train=True, download=True,transform=transform)
    dataset_test  = datasets.MNIST(f'{workarea}/data' , train=False, transform=transform)

    train_loader = torch.utils.data.DataLoader(dataset_train,shuffle=True, batch_size=batch_size)
    test_loader  = torch.utils.data.DataLoader(dataset_test, shuffle=True, batch_size=test_batch_size)

    model     = Net().to(device)
    optimizer = optim.Adadelta(model.parameters(), lr=lr)
    scheduler = StepLR(optimizer, step_size=1, gamma=gamma)
    
    
    for epoch in range(epochs):
        train(tracking, run_id, model, device, train_loader, optimizer, epoch)
        test(tracking, run_id, model, device, test_loader)
        scheduler.step()


    with mlflow.start_run(run_id):
        mlflow.pytorch.log_model(model, "model")



    logger.info('Finish job...')
    sys.exit(0)


if __name__ == '__main__':
    main()










